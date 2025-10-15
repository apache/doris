#include "interaction_channel.h"

#include <fcntl.h>
#include <unistd.h>

InteractionChannel::InteractionChannel(int read_fd, int write_fd)
        : _read_fd(read_fd), _write_fd(write_fd) {}

InteractionChannel::InteractionChannel(InteractionChannel&& other) noexcept
        : _read_fd(other._read_fd), _write_fd(other._write_fd) {
    other._read_fd = -1;
    other._write_fd = -1;
}

InteractionChannel& InteractionChannel::operator=(InteractionChannel&& other) noexcept {
    if (this != &other) {
        release();
        _read_fd = other._read_fd;
        _write_fd = other._write_fd;
        other._read_fd = -1;
        other._write_fd = -1;
    }
    return *this;
}

InteractionChannel::~InteractionChannel() {
    release();
}

ChannelInfo create_channel_info_for_ipc() {
    int pipe_req[2], pipe_resp[2];
    if (pipe(pipe_req) < 0 || pipe(pipe_resp) < 0) {
        // Return all members as -1 to indicate failure
        return ChannelInfo { {-1,-1}, {-1,-1}};
    }
    return ChannelInfo {{pipe_req[0],pipe_req[1]}, {pipe_resp[0],pipe_resp[1]}};
}

/**
 * Helper function to check if a file descriptor is valid and not a standard stream.
 * Valid file descriptors must be >= 3 to avoid conflicts with stdin(0), stdout(1), stderr(2).
 *
 * @param fd The file descriptor to validate
 * @return true if the file descriptor is valid, false otherwise
 */
static bool is_valid_fd(int fd) {
    return fd >= 3 && fcntl(fd, F_GETFD) != -1;
}

bool InteractionChannel::send(const void* data, size_t size) const {
    size_t sent = 0;
    while (sent < size) {
        // No need to flush: write() is a system call and writes data directly to the kernel buffer.
        ssize_t n = write(_write_fd, (const char*)data + sent, size - sent);
        if (n <= 0) {
            return false;
        }
        sent += n;
    }
    return true;
}

bool InteractionChannel::recv(void* data, size_t size) const {
    size_t recvd = 0;
    while (recvd < size) {
        // No need to flush: read() is a system call and reads data directly from the kernel buffer.
        ssize_t n = read(_read_fd, (char*)data + recvd, size - recvd);
        if (n <= 0) {
            return false;
        }
        recvd += n;
    }
    return true;
}

void InteractionChannel::release() {
    if (is_valid_fd(_read_fd)) {
        close(_read_fd);
        _read_fd = -1;
    }
    if (is_valid_fd(_write_fd)) {
        close(_write_fd);
        _write_fd = -1;
    }
}

bool InteractionChannel::is_valid() const {
    return is_valid_fd(_read_fd) || is_valid_fd(_write_fd);
}
