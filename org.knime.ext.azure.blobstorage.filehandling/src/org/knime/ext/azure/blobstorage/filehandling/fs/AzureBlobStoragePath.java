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
 *   2020-07-14 (Alexander Bondaletov): created
 */
package org.knime.ext.azure.blobstorage.filehandling.fs;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.regex.Pattern;

import org.knime.filehandling.core.connections.base.BaseFileSystem;
import org.knime.filehandling.core.connections.base.BlobStorePath;

/**
 * {@link Path} implementation for the {@link AzureBlobStorageFileSystem}.
 *
 * @author Alexander Bondaletov
 */
public class AzureBlobStoragePath extends BlobStorePath {

    private static final Pattern VALID_CONTAINER_NAME = Pattern.compile("^(\\w|\\w-\\w)*$");

    /**
     * Creates path from the given path string.
     *
     * @param fileSystem
     *            the file system.
     * @param first
     *            The first name component.
     * @param more
     *            More name components. the string representation of the path.
     */
    public AzureBlobStoragePath(final BaseFileSystem<?> fileSystem, final String first, final String[] more) {
        super(fileSystem, first, more);
        validateContainerName();
    }

    /**
     * Creates path from the given bucket name and the object key.
     *
     * @param fileSystem
     *            the file system.
     * @param bucketName
     *            the bucket name.
     * @param blobName
     *            the object key.
     */
    public AzureBlobStoragePath(final BaseFileSystem<?> fileSystem, final String bucketName, final String blobName) {
        super(fileSystem, bucketName, blobName);
        validateContainerName();
    }

    private void validateContainerName() {
        if (isAbsolute()) {
            String container = getBucketName();
            if (container != null) {
                if (!VALID_CONTAINER_NAME.matcher(container).matches()) {
                    throw new InvalidPathException(toString(), "Invalid container name");
                }
                if (container.length() < 3) {
                    throw new InvalidPathException(toString(), "Container name is too short");
                }
                if (container.length() > 63) {
                    throw new InvalidPathException(toString(), "Container name is too long");
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AzureBlobStorageFileSystem getFileSystem() {
        return (AzureBlobStorageFileSystem) super.getFileSystem();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AzureBlobStoragePath toDirectoryPath() {
        return (AzureBlobStoragePath) super.toDirectoryPath();
    }

    @SuppressWarnings("resource")
    @Override
    protected boolean lastComponentUsesRelativeNotation() {
        if (getFileSystem().normalizePaths()) {
            return super.lastComponentUsesRelativeNotation();
        }
        return false;
    }

    @Override
    public Path normalize() {
        if (getFileSystem().normalizePaths()) {
            return super.normalize();
        } else {
            return this;
        }
    }

    @SuppressWarnings("resource")
    @Override
    public Path relativize(final Path other) {
        if (!getFileSystem().normalizePaths()) {
            throw new IllegalArgumentException("Cannot relativize paths if normalization is disabled.");
        }

        return super.relativize(other);
    }
}
