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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.util.Iterator;
import java.util.Set;

import org.knime.ext.azure.blobstorage.filehandling.AzureUtils;
import org.knime.filehandling.core.connections.base.BaseFileSystemProvider;
import org.knime.filehandling.core.connections.base.attributes.BaseFileAttributes;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobContainerProperties;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;

/**
 * File system provider for the {@link AzureBlobStorageFileSystem}.
 *
 * @author Alexander Bondaletov
 */
public class AzureBlobStorageFileSystemProvider
        extends BaseFileSystemProvider<AzureBlobStoragePath, AzureBlobStorageFileSystem> {
    /**
     * Azure Blob Storage URI scheme.
     */
    public static final String FS_TYPE = "microsoft-blobstorage";

    /**
     * {@inheritDoc}
     */
    @Override
    protected SeekableByteChannel newByteChannelInternal(final AzureBlobStoragePath path, final Set<? extends OpenOption> options,
            final FileAttribute<?>... attrs) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void moveInternal(final AzureBlobStoragePath source, final AzureBlobStoragePath target, final CopyOption... options)
            throws IOException {
        // TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void copyInternal(final AzureBlobStoragePath source, final AzureBlobStoragePath target, final CopyOption... options)
            throws IOException {
        // TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected InputStream newInputStreamInternal(final AzureBlobStoragePath path, final OpenOption... options) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected OutputStream newOutputStreamInternal(final AzureBlobStoragePath path, final OpenOption... options)
            throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Iterator<AzureBlobStoragePath> createPathIterator(final AzureBlobStoragePath dir, final Filter<? super Path> filter)
            throws IOException {
        return AzureBlobStoragePathIteratorFactory.create(dir, filter);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("resource")
    @Override
    protected void createDirectoryInternal(final AzureBlobStoragePath dir, final FileAttribute<?>... attrs) throws IOException {
        BlobContainerClient contClient = dir.getFileSystem().getClient().getBlobContainerClient(dir.getBucketName());


        if (dir.getBlobName() != null) {
            contClient.getBlobClient(dir.toDirectoryPath().getBlobName()).upload(new ByteArrayInputStream(new byte[0]),
                    0);
        } else {
            contClient.create();
        }
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("resource")
    @Override
    protected boolean exists(final AzureBlobStoragePath path) throws IOException {
        if (path.getBucketName() == null) {
            // This is the fake root
            return true;
        }

        try {
            BlobContainerClient contClient = path.getFileSystem().getClient()
                    .getBlobContainerClient(path.getBucketName());

            if (path.getBlobName() == null) {
                return contClient.exists();
            }

            boolean exist = contClient.getBlobClient(path.getBlobName()).exists();
            if (!exist) {
                ListBlobsOptions opts = new ListBlobsOptions().setPrefix(path.toDirectoryPath().getBlobName())
                        .setMaxResultsPerPage(1);
                exist = contClient.listBlobsByHierarchy(path.getFileSystem().getSeparator(), opts, null).iterator()
                        .hasNext();
            }

            return exist;
        } catch (BlobStorageException ex) {
            throw AzureUtils.toIOE(ex, path.toString());
        }
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("resource")
    @Override
    protected BaseFileAttributes fetchAttributesInternal(final AzureBlobStoragePath path, final Class<?> type)
            throws IOException {
        FileTime createdAt = FileTime.fromMillis(0);
        FileTime modifiedAt = createdAt;
        long size = 0;
        boolean objectExists = false;

        if (path.getBucketName() != null) {
            try {
                BlobContainerClient contClient = path.getFileSystem().getClient()
                        .getBlobContainerClient(path.getBucketName());

                if (path.getBlobName() != null) {
                    BlobClient blobClient = contClient.getBlobClient(path.getBlobName());
                    objectExists = blobClient.exists();

                    if (objectExists) {
                        BlobProperties p = blobClient.getProperties();
                        createdAt = FileTime.from(p.getCreationTime().toInstant());
                        modifiedAt = FileTime.from(p.getLastModified().toInstant());
                        size = p.getBlobSize();
                    }
                } else {
                    BlobContainerProperties p = contClient.getProperties();
                    modifiedAt = FileTime.from(p.getLastModified().toInstant());
                }
            } catch (BlobStorageException ex) {
                throw AzureUtils.toIOE(ex, path.toString());
            }
        }

        return new BaseFileAttributes(!path.isDirectory() && objectExists, path, modifiedAt, modifiedAt, createdAt,
                size, false, false, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void checkAccessInternal(final AzureBlobStoragePath path, final AccessMode... modes) throws IOException {
        // TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void deleteInternal(final AzureBlobStoragePath path) throws IOException {
        // TODO Auto-generated method stub

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getScheme() {
        return FS_TYPE;
    }

}
