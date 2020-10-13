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
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Pattern;

import org.knime.ext.azure.blobstorage.filehandling.AzureUtils;
import org.knime.filehandling.core.connections.FSFiles;
import org.knime.filehandling.core.connections.base.BaseFileSystemProvider;
import org.knime.filehandling.core.connections.base.attributes.BaseFileAttributes;

import com.azure.core.util.polling.SyncPoller;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobContainerProperties;
import com.azure.storage.blob.models.BlobCopyInfo;
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

    private static final Pattern VALID_CONTAINER_NAME_PATTERN = Pattern.compile("^(\\w|\\w-\\w)*$");

    /**
     * {@inheritDoc}
     */
    @Override
    protected SeekableByteChannel newByteChannelInternal(final AzureBlobStoragePath path, final Set<? extends OpenOption> options,
            final FileAttribute<?>... attrs) throws IOException {

        // we cannot read/write a file below the root (only containers can be there)
        if (path.getParent() != null && ((AzureBlobStoragePath) path.getParent()).isRoot()) {
            throw new IOException("Cannot read/write files below the root. Only folders can be there.");
        }

        return new AzureBlobStorageSeekableByteChannel(path, options);
    }

    private boolean isDirectory(final AzureBlobStoragePath path) throws IOException {
        return readAttributes(path, BasicFileAttributes.class).isDirectory();
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("resource")
    @Override
    protected void copyInternal(final AzureBlobStoragePath source, final AzureBlobStoragePath target, final CopyOption... options)
            throws IOException {
        if (isDirectory(source)) {
            if (FSFiles.isNonEmptyDirectory(target)) {
                throw new DirectoryNotEmptyException(
                        String.format("Target directory %s exists and is not empty", target.toString()));
            }

            if (!existsCached(target)) {
                createDirectory(target);
            }
        } else {
            try {
                AzureBlobStorageFileSystem fs = getFileSystemInternal();
                BlobServiceClient client = fs.getClient();
                BlobClient sourceBlobClient = client.getBlobContainerClient(source.getBucketName())
                        .getBlobClient(source.getBlobName());
                BlobClient targetBlobClient = client.getBlobContainerClient(target.getBucketName())
                        .getBlobClient(target.getBlobName());

                SyncPoller<BlobCopyInfo, Void> poller = targetBlobClient.beginCopy(sourceBlobClient.getBlobUrl(), null);
                poller.waitForCompletion();
            } catch (BlobStorageException ex) {
                throw AzureUtils.toIOE(ex, source.toString(), target.toString());
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("resource")
    @Override
    protected InputStream newInputStreamInternal(final AzureBlobStoragePath path, final OpenOption... options) throws IOException {
        try {
            return path.getFileSystem()
                    .getBlobClientwithIncreasedTimeout(path.getBucketName(), path.getBlobName(), Files.size(path))
                    .openInputStream();
        } catch (BlobStorageException ex) {
            throw AzureUtils.toIOE(ex, path.toString());
        }
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("resource")
    @Override
    protected OutputStream newOutputStreamInternal(final AzureBlobStoragePath path, final OpenOption... options)
            throws IOException {

        final Set<OpenOption> opts = new HashSet<>(Arrays.asList(options));
        return Channels.newOutputStream(newByteChannel(path, opts));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Iterator<AzureBlobStoragePath> createPathIterator(final AzureBlobStoragePath dir, final Filter<? super Path> filter)
            throws IOException {
        return AzureBlobStoragePathIteratorFactory.create(dir, filter);
    }



    private static void validateContainerName(final String container) throws IOException {
        if (!VALID_CONTAINER_NAME_PATTERN.matcher(container).matches()) {
            throw new IOException(String.format(
                    "Invalid Azure Blob Storage container name: %s (only allowed characters are letters, numbers, and '-')",
                    container));
        }
        if (container.length() < 3) {
            throw new IOException(String.format(
                    "Invalid Azure Blob Storage container name: %s (must have at least three characters)", container));
        }
        if (container.length() > 63) {
            throw new IOException(String.format(
                    "Invalid Azure Blob Storage container name: %s (must only have up to 63 characters)", container));
        }
    }

    private static boolean isContainerNameValid(final String container) {
        return VALID_CONTAINER_NAME_PATTERN.matcher(container).matches() //
                && container.length() >= 3 //
                && container.length() <= 63;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("resource")
    @Override
    protected void createDirectoryInternal(final AzureBlobStoragePath dir, final FileAttribute<?>... attrs) throws IOException {

        if (dir.getBucketName() != null && dir.getBlobName() == null) {
            // before creating a BS container we should validate the name (to prevent funny
            // exceptions in the BlobContainerClient)
            validateContainerName(dir.getBucketName());
        }

        final AzureBlobStorageFileSystem fs = dir.getFileSystem();
        final BlobContainerClient contClient = fs.getClient().getBlobContainerClient(dir.getBucketName());

        try {
            if (dir.getBlobName() != null) {
                contClient.getBlobClient(dir.toDirectoryPath().getBlobName())
                        .upload(new ByteArrayInputStream(new byte[0]), 0, true);
            } else {
                contClient.create();
            }
        } catch (BlobStorageException ex) {
            throw AzureUtils.toIOE(ex, dir.toString());
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

        if (!isContainerNameValid(path.getBucketName())) {
            return false;
        }

        try {
            AzureBlobStorageFileSystem fs = path.getFileSystem();
            BlobContainerClient contClient = fs.getClient().getBlobContainerClient(path.getBucketName());

            if (path.getBlobName() == null) {
                return contClient.exists();
            }

            boolean exist = contClient.getBlobClient(path.getBlobName()).exists();
            if (!exist) {
                ListBlobsOptions opts = new ListBlobsOptions().setPrefix(path.toDirectoryPath().getBlobName())
                        .setMaxResultsPerPage(1);
                exist = contClient.listBlobsByHierarchy(fs.getSeparator(), opts, null).iterator().hasNext();
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
                if (!isContainerNameValid(path.getBucketName())) {
                    throw new NoSuchFileException(path.toString());
                }

                AzureBlobStorageFileSystem fs = path.getFileSystem();
                BlobContainerClient contClient = fs.getClient().getBlobContainerClient(path.getBucketName());

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
    @SuppressWarnings("resource")
    @Override
    protected void deleteInternal(final AzureBlobStoragePath path) throws IOException {
        String blobName = path.getBlobName();
        if (isDirectory(path)) {
            blobName = path.toDirectoryPath().getBlobName();
            if (FSFiles.isNonEmptyDirectory(path)) {
                throw new DirectoryNotEmptyException(path.toString());
            }
        }

        AzureBlobStorageFileSystem fs = path.getFileSystem();
        BlobContainerClient contClient = fs.getClient().getBlobContainerClient(path.getBucketName());

        try {
            if (path.getBlobName() != null) {
                contClient.getBlobClient(blobName).delete();
            } else {
                contClient.delete();
            }
        } catch (BlobStorageException ex) {
            throw AzureUtils.toIOE(ex, path.toString());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getScheme() {
        return FS_TYPE;
    }

}
