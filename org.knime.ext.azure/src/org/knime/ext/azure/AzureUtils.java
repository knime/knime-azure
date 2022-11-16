/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   2020-07-20 (Alexander Bondaletov): created
 */
package org.knime.ext.azure;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.knime.core.node.NodeLogger;
import org.knime.ext.microsoft.authentication.port.MicrosoftCredential;
import org.knime.ext.microsoft.authentication.port.azure.storage.AzureSasTokenCredential;
import org.knime.ext.microsoft.authentication.port.azure.storage.AzureSharedKeyCredential;
import org.knime.ext.microsoft.authentication.port.oauth2.OAuth2Credential;

import com.azure.core.exception.HttpResponseException;
import com.azure.core.http.ProxyOptions;
import com.azure.core.util.ConfigurationBuilder;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Utility class for Azure Blob Storage and Datalake Storage
 *
 * @author Alexander Bondaletov
 */
public final class AzureUtils {

    private AzureUtils() {
    }

    private static final NodeLogger LOGGER = NodeLogger.getLogger(AzureUtils.class);

    private static final Pattern ERROR_PATTERN = Pattern.compile(".*<Message>([^\n]*)\n.*", Pattern.DOTALL);
    private static final Pattern BLOB_STORAGE_SCOPE_PATTERN = Pattern.compile("https://[^.]+.blob.core.windows.net/.+");

    private static final Pattern[] INVALID_CREDENTIAL_MESSAGES = new Pattern[] {
            Pattern.compile(
                    "The MAC signature found in the HTTP request .* is not the same as any computed signature."), //
            Pattern.compile("Signature fields not well formed"), //
            Pattern.compile("<Error>.*Signature did not match.*</Error>"), //
            Pattern.compile(".* is mandatory. Cannot be empty") };

    private static final String HTTPS = "https";
    private static final String HTTP = "http";
    private static final String JAVA_PROXY_HOST = "proxyHost";

    /**
     * Extracts human readable error message from the {@link HttpResponseException}.
     *
     * @param ex
     *            The azure blob storage exception
     * @return Human-readable error message.
     */
    public static String parseErrorMessage(final HttpResponseException ex) {
        Matcher m = ERROR_PATTERN.matcher(ex.getMessage());
        if (m.matches()) {
            return m.group(1);
        }
        return ex.getMessage();
    }

    /**
     * Makes an attempt to derive an appropriate {@link IOException} from the
     * response status code of provided {@link HttpResponseException}.
     *
     * Returns wrapped {@link HttpResponseException} with human readable error
     * message otherwise.
     *
     * @param ex
     *            The {@link HttpResponseException} instance.
     * @param file
     *            A string identifying the file or {@code null} if not known.
     * @param other
     *            A string identifying the other file or {@code null} if not known.
     * @return Appropriate {@link IOException} or the wrapped
     *         {@link HttpResponseException} with the human-readable error message.
     */
    @SuppressWarnings("resource")
    public static IOException toIOE(final HttpResponseException ex, final String file, final String other) {
        String message = parseErrorMessage(ex);

        if (ex.getResponse().getStatusCode() == HttpResponseStatus.NOT_FOUND.code()) {
            NoSuchFileException nsfe = new NoSuchFileException(file, other, message);
            nsfe.initCause(ex);
            return nsfe;
        }
        if (ex.getResponse().getStatusCode() == HttpResponseStatus.FORBIDDEN.code()) {
            AccessDeniedException ade = new AccessDeniedException(file, other, "Access denied");
            ade.initCause(ex);
            return ade;
        }

        return new WrappedHttpResponseException(message, ex);
    }

    /**
     * Makes an attempt to derive an appropriate {@link IOException} from the
     * response status code of provided {@link HttpResponseException}.
     *
     * Returns wrapped {@link HttpResponseException} with human readable error
     * message otherwise.
     *
     * @param ex
     *            The {@link HttpResponseException} instance.
     * @param file
     *            A string identifying the file or {@code null} if not known.
     * @return Appropriate {@link IOException} or the wrapped
     *         {@link HttpResponseException} with the human-readable error message.
     */
    public static IOException toIOE(final HttpResponseException ex, final String file) {
        return toIOE(ex, file, null);
    }

    /**
     * Makes and attempt to recognize invalid credentials error based on the
     * exception message and throws it as {@link IOException}.
     *
     * No exception is throws in case provided exception is 403 error, but the
     * message does not match with any of the known messages.
     *
     * @param ex
     *            The exception.
     * @throws IOException
     */
    @SuppressWarnings("resource")
    public static void handleAuthException(final HttpResponseException ex) throws IOException {
        if (ex.getResponse().getStatusCode() == HttpResponseStatus.FORBIDDEN.code()) {
            if (Arrays.stream(INVALID_CREDENTIAL_MESSAGES).anyMatch(p -> p.matcher(ex.getMessage()).find())) {
                throw new IOException(
                        "Authentication failure. Please check your credentials in the Microsoft Authentication node.",
                        ex);
            } else {
                // there are probably more situations where authentication fails, which are not
                // accounted for in INVALID_CREDENTIAL_MESSAGES. To facilitate debugging we are
                // logging the
                // actual exception
                LOGGER.debug("Failed to list containers in storage account: " + ex.getMessage(), ex);
            }
        } else {
            throw toIOE(ex, "");
        }
    }

    /**
     * Retrieves the storage account from provided {@link MicrosoftCredential}.
     *
     * @param credential
     *            The credential.
     * @return The storage account.
     */
    public static String getStorageAccount(final MicrosoftCredential credential) {
        switch (credential.getType()) {
        case AZURE_SHARED_KEY:
            final AzureSharedKeyCredential sharedKeyCredential = (AzureSharedKeyCredential) credential;
            return sharedKeyCredential.getAccount();
        case AZURE_SAS_TOKEN:
            try {
                return extractStorageAccount(((AzureSasTokenCredential) credential).getSasUrl());
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        case OAUTH2_ACCESS_TOKEN:
            final OAuth2Credential oauth2Credential = (OAuth2Credential) credential;
            return extractStorageAccount(extractEndpoint(oauth2Credential));
        default:
            throw new UnsupportedOperationException("Unsupported credential type " + credential.getType());
        }
    }

    private static String extractStorageAccount(final String urlString) {
        final URI url = URI.create(urlString);
        final String[] hostnameParts = url.getHost().split("\\.");
        return hostnameParts[0];
    }

    private static String extractEndpoint(final OAuth2Credential credential) {
        final String scope = credential.getScopes().stream() //
                .filter(BLOB_STORAGE_SCOPE_PATTERN.asPredicate()) //
                .findFirst() //
                .orElseThrow(() -> new IllegalStateException(
                        "Credentials do not provide access to Azure Blob Storage. Please request the appropriate access in the Microsoft Authentication node."));

        try {
            return "https://" + new URL(scope).getHost();
        } catch (MalformedURLException ex) {
            throw new UnsupportedOperationException("Malformed scope: " + scope, ex);
        }
    }

    /**
     * Retrieves the endpoint URL from the provided {@link MicrosoftCredential}.
     *
     * @param credential
     *            The credential.
     * @return The endpoint URL.
     * @throws IOException
     */
    public static String getEndpoint(final MicrosoftCredential credential) throws IOException {
        switch (credential.getType()) {
        case AZURE_SHARED_KEY:
            return ((AzureSharedKeyCredential) credential).getEndpoint();
        case AZURE_SAS_TOKEN:
            return ((AzureSasTokenCredential) credential).getSasUrl();
        case OAUTH2_ACCESS_TOKEN:
            return extractEndpoint((OAuth2Credential) credential);
        default:
            throw new UnsupportedOperationException("Unsupported credential type " + credential.getType());
        }
    }

    /**
     * Check if proxy is active.
     *
     * @return true if proxy is active otherwise false
     */
    public static boolean isProxyActive() {
        final var httpsHost = loadSystemProperty(HTTPS + "." + JAVA_PROXY_HOST);
        final var httpHost = loadSystemProperty(HTTP + "." + JAVA_PROXY_HOST);
        return (httpsHost != null && !httpsHost.isEmpty()) || (httpHost != null && !httpHost.isEmpty());
    }

    /**
     * Retrieve proxy options from system properties
     *
     * @return proxy options {@link ProxyOptions}
     */
    public static ProxyOptions loadSystemProxyOptions() {
        // create empty configurations
        final var emptyConfig = new ConfigurationBuilder(s -> Map.of(), s -> Map.of(), s -> Map.of()).build();
        // because of an empty configuration,
        // proxy configurations are loaded from system properties not from static
        // immutable {@link Configuration#getGlobalConfiguration()}
        return ProxyOptions.fromConfiguration(emptyConfig, true);
    }

    private static String loadSystemProperty(final String name) {
        return System.getProperty(name);
    }

    /**
     *
     * Wrapper for the {@link HttpResponseException} with the human readable error
     * message extracted.
     */
    public static final class WrappedHttpResponseException extends IOException {
        private static final long serialVersionUID = 1L;

        private WrappedHttpResponseException(final String message, final HttpResponseException cause) {
            super(message, cause);
        }
    }
}
