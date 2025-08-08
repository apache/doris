package org.apache.doris.datasource.credentials;

import com.google.common.annotations.VisibleForTesting;

import java.util.Map;

public class CredentialUtils {
    /**
     * Future method for FileIO-based credential extraction.
     * This method signature is designed to be compatible with future FileIO implementations.
     *
     * @param fileIoProperties properties from FileIO (reserved for future use)
     * @param extractor custom credential extractor
     * @return extracted credentials
     */
    @VisibleForTesting
    public static Map<String, String> extractCredentialsFromFileIO(Map<String, String> fileIoProperties,
            CredentialExtractor extractor) {
        return extractor.extractCredentials(fileIoProperties);
    }
}
