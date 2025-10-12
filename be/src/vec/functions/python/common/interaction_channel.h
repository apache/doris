#pragma once
#include <string>

/**
 * {@link InteractionChannel} provides a communication channel between parent and child processes
 * using file descriptors for IPC (Inter-Process Communication).
 */
class InteractionChannel {
public:
    /**
     * Constructs an InteractionChannel with specified read and write file descriptors.
     *
     * @param read_fd The file descriptor for reading data from the other process
     * @param write_fd The file descriptor for writing data to the other process
     */
    InteractionChannel(int read_fd = -1, int write_fd = -1);
    // Disable copy constructor and copy assignment operator to prevent resource management issues.
    // Since {@link InteractionChannel} manages file descriptors (system resources), copying would
    // lead to multiple objects owning the same file descriptors, causing double-close problems.
    InteractionChannel(const InteractionChannel&) = delete;
    InteractionChannel& operator=(const InteractionChannel&) = delete;
    // Enable move constructor and move assignment operator for efficient resource transfer.
    // These allow transferring ownership of file descriptors between objects without copying,
    // which is essential for RAII pattern and prevents resource leaks.
    InteractionChannel(InteractionChannel&& other) noexcept;
    InteractionChannel& operator=(InteractionChannel&& other) noexcept;
    ~InteractionChannel();

    /**
     * Sends data through the write file descriptor to the other process.
     *
     * @param data Pointer to the data to be sent
     * @param size Size of the data in bytes
     * @return true if the data was sent successfully, false otherwise
     */
    bool send(const void* data, size_t size) const;

    /**
     * Receives data from the read file descriptor from the other process.
     *
     * @param data Pointer to the buffer where received data will be stored
     * @param size Size of the data to receive in bytes
     * @return true if the data was received successfully, false otherwise
     */
    bool recv(void* data, size_t size) const;

    /**
     * Releases the file descriptors by closing them. This method ensures
     * that the file descriptors are properly cleaned up to prevent resource leaks.
     */
    void release();

    /**
     * Get the valid state of channel.
     *
     * @return if the channel is valid.
     */
    bool is_valid() const;

private:
    int _read_fd;
    int _write_fd;
};

/**
 * {@link ChannelInfo} contains the communication channel and file descriptors needed
 * for setting up IPC between parent and child processes.
 */
typedef struct ChannelInfo {
    int pipe_req[2];
    int pipe_resp[2];
} ChannelInfo;

/**
 * Creates a pair of pipes for IPC communication and returns the channel information.
 * This method sets up the communication infrastructure between parent and child processes.
 *
 * @return {@link ChannelInfo} containing the channel and file descriptors for child process
 */
struct ChannelInfo create_channel_info_for_ipc();