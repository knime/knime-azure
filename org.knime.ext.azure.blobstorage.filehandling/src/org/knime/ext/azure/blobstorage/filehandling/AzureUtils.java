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
package org.knime.ext.azure.blobstorage.filehandling;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.azure.storage.blob.models.BlobStorageException;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Utility class for Azure Blob Storage
 *
 * @author Alexander Bondaletov
 */
public class AzureUtils {

    private static final Pattern ERROR_PATTERN = Pattern.compile(".*<Message>([^\n]*)\n.*", Pattern.DOTALL);

    /**
     * Extracts human readable error message from the {@link BlobStorageException}.
     *
     * @param ex
     *            The azure blob storage exception
     * @return Human-readable error message.
     */
    public static String parseErrorMessage(final BlobStorageException ex) {
        Matcher m = ERROR_PATTERN.matcher(ex.getMessage());
        if (m.matches()) {
            return m.group(1);
        }
        return ex.getMessage();
    }

    /**
     * Makes an attempt to derive an appropriate {@link IOException} from the
     * response status code of provided {@link BlobStorageException}.
     *
     * Returns wrapped {@link BlobStorageException} with human readable error
     * message otherwise.
     *
     * @param ex
     *            The {@link BlobStorageException} instance.
     * @param file
     *            A string identifying the file or {@code null} if not known.
     * @param other
     *            A string identifying the other file or {@code null} if not known.
     * @return Appropriate {@link IOException} or the wrapped
     *         {@link BlobStorageException} with the human-readable error message.
     */
    public static IOException toIOE(final BlobStorageException ex, final String file, final String other) {
        String message = parseErrorMessage(ex);

        if (ex.getStatusCode() == HttpResponseStatus.NOT_FOUND.code()) {
            NoSuchFileException nsfe = new NoSuchFileException(file, other, message);
            nsfe.initCause(ex);
            return nsfe;
        }
        if (ex.getStatusCode() == HttpResponseStatus.FORBIDDEN.code()) {
            AccessDeniedException ade = new AccessDeniedException(file, other, "Access denied");
            ade.initCause(ex);
            return ade;
        }

        return new WrappedBlobStorageException(message, ex);
    }

    /**
     * Makes an attempt to derive an appropriate {@link IOException} from the
     * response status code of provided {@link BlobStorageException}.
     *
     * Returns wrapped {@link BlobStorageException} with human readable error
     * message otherwise.
     *
     * @param ex
     *            The {@link BlobStorageException} instance.
     * @param file
     *            A string identifying the file or {@code null} if not known.
     * @return Appropriate {@link IOException} or the wrapped
     *         {@link BlobStorageException} with the human-readable error message.
     */
    public static IOException toIOE(final BlobStorageException ex, final String file) {
        return toIOE(ex, file, null);
    }

    /**
     *
     * Wrapper for the {@link BlobStorageException} with the human readable error
     * message extracted.
     */
    public static class WrappedBlobStorageException extends IOException {
        private static final long serialVersionUID = 1L;

        private WrappedBlobStorageException(final String message, final BlobStorageException cause) {
            super(message, cause);
        }
    }
}
