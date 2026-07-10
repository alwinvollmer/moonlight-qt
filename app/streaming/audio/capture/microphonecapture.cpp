#include "microphonecapture.h"
#include "sdlmicrophonecapture.h"

#include <Limelight.h>
#include <SDL.h>
#include <QDebug>
#include <QLoggingCategory>
#include <QThread>
#include <QAbstractEventDispatcher>
#include <QHostAddress>
#include <QNetworkDatagram>

#include <chrono>
#include <cstring>
#ifdef Q_OS_WIN
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#endif

MicrophoneCapture::MicrophoneCapture(QObject *parent)
    : QObject(parent)
    , m_ServerPort(0)
    , m_Enabled(false)
    , m_IsStreaming(false)
    , m_Gain(1.0f)
    , m_AudioCapture(nullptr)
    , m_SampleRate(OPUS_SAMPLE_RATE)
    , m_Channels(OPUS_CHANNELS)
    , m_FrameSize(0)
    , m_OpusEncoder(nullptr)
    , m_EncodedBuffer(nullptr)
    , m_EncodedBufferSize(0)
    , m_UdpSocket(nullptr)
    , m_CaptureTimer(nullptr)
    , m_SendFd(-1)
    , m_Running(false)
{
}

MicrophoneCapture::~MicrophoneCapture()
{
    cleanupResources();
}

bool MicrophoneCapture::initialize(const QString& serverAddress, int serverPort, 
                                 const STREAM_CONFIGURATION& streamConfig)
{
    m_ServerAddress = serverAddress;
    m_ServerPort = serverPort;

    // Faithful passthrough: send the mic at its captured (local) level so the
    // level matches what the mic produces locally, and volume is adjusted on the
    // host side (e.g. the mic slider on the host). Default gain 1.0 = no change.
    // MOONLIGHT_MIC_GAIN=<n> optionally applies a fixed multiplier if ever wanted.
    m_Gain = 1.0f;
    QByteArray gainEnv = qgetenv("MOONLIGHT_MIC_GAIN");
    if (!gainEnv.isEmpty()) {
        bool ok = false;
        float parsed = QString::fromUtf8(gainEnv).toFloat(&ok);
        if (ok && parsed > 0.0f) {
            m_Gain = parsed;
        }
    }
    qCInfo(QLoggingCategory("microphone")) << "Microphone gain:" << m_Gain;

    // Calculate frame size for 20ms at 48kHz
    m_FrameSize = (OPUS_SAMPLE_RATE * OPUS_FRAME_MS) / 1000;

    // Initialize UDP socket
    m_UdpSocket = new QUdpSocket(this);
    if (!m_UdpSocket->bind()) {
        qCWarning(QLoggingCategory("microphone")) << "Failed to bind UDP socket for microphone";
        return false;
    }

    // Raw UDP socket for the actual sending. We send from a dedicated std::thread
    // (the QTimer never fires during streaming), and QUdpSocket is not safe to use
    // across threads, so use a plain fd + sendto instead.
    m_SendFd = static_cast<qintptr>(::socket(AF_INET, SOCK_DGRAM, 0));
#ifdef Q_OS_WIN
    if (m_SendFd == static_cast<qintptr>(INVALID_SOCKET)) {
#else
    if (m_SendFd < 0) {
#endif
        qCWarning(QLoggingCategory("microphone")) << "Failed to create raw UDP send socket";
        return false;
    }

    // Initialize capture timer
    m_CaptureTimer = new QTimer(this);
    m_CaptureTimer->setInterval(OPUS_FRAME_MS); // 20ms intervals
    connect(m_CaptureTimer, &QTimer::timeout, this, &MicrophoneCapture::captureAndStreamAudio);

    // Initialize Opus encoder
    if (!initializeOpusEncoder()) {
        qCWarning(QLoggingCategory("microphone")) << "Failed to initialize Opus encoder";
        return false;
    }

    // Create audio capture
    if (!createAudioCapture()) {
        qCWarning(QLoggingCategory("microphone")) << "Failed to create audio capture";
        return false;
    }

    qCInfo(QLoggingCategory("microphone")) << "Microphone capture initialized for" << serverAddress << ":" << serverPort;
    return true;
}

bool MicrophoneCapture::start()
{
    if (m_IsStreaming || !m_Enabled) {
        return false;
    }

    if (!m_AudioCapture || !m_OpusEncoder) {
        qCWarning(QLoggingCategory("microphone")) << "Microphone not properly initialized";
        return false;
    }

    // Start audio capture
    if (!m_AudioCapture->startCapture()) {
        qCWarning(QLoggingCategory("microphone")) << "Failed to start audio capture";
        return false;
    }

    // Drive sending from a dedicated thread. The QTimer slot never runs during
    // streaming because this thread's Qt event loop is blocked in the stream loop.
    m_IsStreaming = true;
    m_Running = true;
    m_SenderThread = std::thread(&MicrophoneCapture::senderLoop, this);

    qCInfo(QLoggingCategory("microphone")) << "Microphone streaming started (sender thread)";
    return true;
}

void MicrophoneCapture::stop()
{
    if (!m_IsStreaming) {
        return;
    }

    // Stop the sender thread
    m_Running = false;
    m_IsStreaming = false;
    if (m_SenderThread.joinable()) {
        m_SenderThread.join();
    }

    // Stop audio capture
    if (m_AudioCapture) {
        m_AudioCapture->stopCapture();
    }

    qCInfo(QLoggingCategory("microphone")) << "Microphone streaming stopped";
}

void MicrophoneCapture::setEnabled(bool enabled)
{
    if (m_Enabled == enabled) {
        return;
    }

    m_Enabled = enabled;
    
    if (!enabled && m_IsStreaming) {
        stop();
    }
    
    qCInfo(QLoggingCategory("microphone")) << "Microphone" << (enabled ? "enabled" : "disabled");
}

void MicrophoneCapture::captureAndStreamAudio()
{
    if (!m_AudioCapture || !m_OpusEncoder || !m_IsStreaming) {
        return;
    }

    void* audioBuffer = nullptr;
    int audioSize = 0;

    static int s_tick = 0, s_got = 0;
    s_tick++;

    // Get audio buffer from capture device
    if (!m_AudioCapture->getAudioBuffer(&audioBuffer, &audioSize)) {
        if ((s_tick % 100) == 1) {
            qCInfo(QLoggingCategory("microphone")) << "DIAG tick" << s_tick << "getAudioBuffer=empty";
        }
        return; // No audio available
    }
    s_got++;
    if ((s_got % 50) == 1) {
        qCInfo(QLoggingCategory("microphone")) << "DIAG tick" << s_tick << "got buffers" << s_got << "size" << audioSize;
    }

    // Encode and send the audio
    encodeAndSendAudio(audioBuffer, audioSize);

    // Release the buffer
    m_AudioCapture->releaseAudioBuffer();
}

void MicrophoneCapture::senderLoop()
{
    int sentCount = 0;
    while (m_Running.load()) {
        // Drain everything currently queued, then sleep briefly.
        void* audioBuffer = nullptr;
        int audioSize = 0;
        bool any = false;
        while (m_Running.load() && m_AudioCapture &&
               m_AudioCapture->getAudioBuffer(&audioBuffer, &audioSize)) {
            if (encodeAndSendAudio(audioBuffer, audioSize)) {
                if ((sentCount++ % 50) == 0) {
                    qCInfo(QLoggingCategory("microphone")) << "DIAG sender: sent" << sentCount << "packets";
                }
            }
            m_AudioCapture->releaseAudioBuffer();
            any = true;
        }
        if (!any) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }
}

bool MicrophoneCapture::createAudioCapture()
{
    // Try SDL audio capture first
    m_AudioCapture = new SdlMicrophoneCapture();
    
    if (m_AudioCapture->initialize(m_SampleRate, m_Channels)) {
        qCInfo(QLoggingCategory("microphone")) << "Using SDL microphone capture";
        return true;
    }

    delete m_AudioCapture;
    m_AudioCapture = nullptr;
    return false;
}

bool MicrophoneCapture::initializeOpusEncoder()
{
    int error;
    
    // Create Opus encoder for mono microphone
    m_OpusEncoder = opus_encoder_create(OPUS_SAMPLE_RATE, OPUS_CHANNELS, 
                                       OPUS_APPLICATION_VOIP, &error);
    
    if (error != OPUS_OK || !m_OpusEncoder) {
        qCWarning(QLoggingCategory("microphone")) << "Failed to create Opus encoder:" << opus_strerror(error);
        return false;
    }

    // Configure encoder
    opus_encoder_ctl(m_OpusEncoder, OPUS_SET_BITRATE(OPUS_BITRATE));
    opus_encoder_ctl(m_OpusEncoder, OPUS_SET_VBR(1));
    opus_encoder_ctl(m_OpusEncoder, OPUS_SET_COMPLEXITY(8));

    // Allocate encoding buffer
    m_EncodedBufferSize = MAX_PACKET_SIZE;
    m_EncodedBuffer = new unsigned char[m_EncodedBufferSize];

    qCInfo(QLoggingCategory("microphone")) << "Opus encoder initialized";
    return true;
}

void MicrophoneCapture::cleanupResources()
{
    stop();

    if (m_AudioCapture) {
        delete m_AudioCapture;
        m_AudioCapture = nullptr;
    }

    if (m_OpusEncoder) {
        opus_encoder_destroy(m_OpusEncoder);
        m_OpusEncoder = nullptr;
    }

    if (m_EncodedBuffer) {
        delete[] m_EncodedBuffer;
        m_EncodedBuffer = nullptr;
    }

    if (m_UdpSocket) {
        m_UdpSocket->close();
        delete m_UdpSocket;
        m_UdpSocket = nullptr;
    }

    if (m_SendFd >= 0) {
#ifdef Q_OS_WIN
        ::closesocket(m_SendFd);
#else
        ::close(m_SendFd);
#endif
        m_SendFd = -1;
    }

    if (m_CaptureTimer) {
        delete m_CaptureTimer;
        m_CaptureTimer = nullptr;
    }
}

bool MicrophoneCapture::encodeAndSendAudio(void* audioData, int audioSize)
{
    if (!m_OpusEncoder || !m_EncodedBuffer || !m_UdpSocket) {
        return false;
    }

    // Calculate expected samples for this frame
    int expectedSamples = m_FrameSize * m_Channels;
    int bytesPerSample = sizeof(short); // Assuming 16-bit samples
    int expectedBytes = expectedSamples * bytesPerSample;

    // Ensure we have the right amount of data
    if (audioSize < expectedBytes) {
        qCDebug(QLoggingCategory("microphone")) << "Insufficient audio data:" << audioSize << "expected:" << expectedBytes;
        return false;
    }

    // Optional fixed gain (default 1.0 = faithful passthrough). Applied in place
    // with saturation. Host-side slider is the intended volume control.
    if (m_Gain != 1.0f) {
        opus_int16* samples = static_cast<opus_int16*>(audioData);
        for (int i = 0; i < expectedSamples; i++) {
            int v = static_cast<int>(samples[i] * m_Gain);
            if (v > 32767) v = 32767;
            else if (v < -32768) v = -32768;
            samples[i] = static_cast<opus_int16>(v);
        }
    }

    // Encode audio with Opus
    int encodedBytes = opus_encode(m_OpusEncoder,
                                  static_cast<const opus_int16*>(audioData),
                                  m_FrameSize,
                                  m_EncodedBuffer,
                                  m_EncodedBufferSize);

    if (encodedBytes <= 0) {
        qCWarning(QLoggingCategory("microphone")) << "Opus encoding failed:" << opus_strerror(encodedBytes);
        return false;
    }

    // Send to server via the raw socket (called from the sender thread; QUdpSocket
    // is not safe to use across threads).
    struct sockaddr_in dest;
    memset(&dest, 0, sizeof(dest));
    dest.sin_family = AF_INET;
    dest.sin_port = htons(static_cast<uint16_t>(m_ServerPort));
    if (inet_pton(AF_INET, m_ServerAddress.toUtf8().constData(), &dest.sin_addr) != 1) {
        qCWarning(QLoggingCategory("microphone")) << "Invalid mic server address:" << m_ServerAddress;
        return false;
    }

    int sent = static_cast<int>(::sendto(m_SendFd,
                            reinterpret_cast<const char*>(m_EncodedBuffer), encodedBytes, 0,
                            reinterpret_cast<struct sockaddr*>(&dest), static_cast<int>(sizeof(dest))));
    if (sent != encodedBytes) {
        qCWarning(QLoggingCategory("microphone")) << "Failed to send audio packet:" << sent << "/" << encodedBytes;
        return false;
    }

    return true;
}