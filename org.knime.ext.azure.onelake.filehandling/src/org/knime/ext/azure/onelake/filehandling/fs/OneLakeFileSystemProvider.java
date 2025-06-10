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
 *   2025-05-03 (Bjoern Lohrmann): created
 */
package org.knime.ext.azure.onelake.filehandling.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import org.knime.ext.azure.AzureUtils;
import org.knime.filehandling.core.connections.FSFiles;
import org.knime.filehandling.core.connections.base.BaseFileSystemProvider;
import org.knime.filehandling.core.connections.base.attributes.BaseFileAttributes;

import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.FileSystemProperties;
import com.azure.storage.file.datalake.models.ListPathsOptions;
import com.azure.storage.file.datalake.models.PathItem;
import com.azure.storage.file.datalake.models.PathProperties;

/**
 * File system provider for the {@link OneLakeFileSystem}.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
class OneLakeFileSystemProvider extends BaseFileSystemProvider<OneLakePath, OneLakeFileSystem> {

    @Override
    protected SeekableByteChannel newByteChannelInternal(final OneLakePath path, //
            final Set<? extends OpenOption> options, //
            final FileAttribute<?>... attrs) throws IOException {

        // we cannot read/write managed paths
        if (path.isManagedPath()) {
            throw new AccessDeniedException(path.toString(), //
                    null, //
                    "cannot read/write files in Fabric-managed locations");
        }

        return new OneLakeSeekableByteChannel(path, options);
    }

    @Override
    protected void copyInternal(final OneLakePath source, final OneLakePath target, final CopyOption... options)
            throws IOException {

        if (FSFiles.isDirectory(source)) {
            if (!existsCached(target)) {
                createDirectory(target);
            }
        } else {
            // ADLS API doesn't have a 'copy' method so we have to do it this way
            try (final var in = newInputStream(source)) {
                Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    @SuppressWarnings("resource")
    @Override
    protected void moveInternal(final OneLakePath source, final OneLakePath target, final CopyOption... options)
            throws IOException {

        if (target.isManagedPath()) {
            throw new AccessDeniedException(target.toString(), //
                    null, //
                    "cannot move files/folders to Fabric-managed locations");
        }

        try {
            if (source.getFilePath() == null || target.getFilePath() == null) {
                moveChildren(source, target);
            } else {
                source.getFileClient().rename(target.getFileSystemName(), target.getFilePath());
                source.getFileSystem().removeFromAttributeCacheDeep(source);
            }
        } catch (DataLakeStorageException ex) {
            throw AzureUtils.toIOE(ex, source.toString(), target.toString());
        }
    }

    @SuppressWarnings("resource")
    private void moveChildren(final OneLakePath source, final OneLakePath target) throws IOException {
        if (!existsCached(target)) {
            createDirectory(target);
        }

        final var fs = source.getFileSystem();
        final var iter = listChildren(source);
        while (iter.hasNext()) {
            PathItem item = iter.next();
            OneLakePath itemPath = fs.getPath(fs.getSeparator() + source.getFileSystemName(), item.getName());

            var destinationPath = itemPath.getFileName().toString();
            if (target.getFilePath() != null) {
                destinationPath = target.getFilePath() + OneLakeFileSystem.PATH_SEPARATOR + destinationPath;
            }

            itemPath.getFileClient().rename(target.getFileSystemName(), destinationPath);
            fs.removeFromAttributeCacheDeep(itemPath);
        }

        delete(source);
    }

    private static Iterator<PathItem> listChildren(final OneLakePath path) {
        final var opts = new ListPathsOptions();
        if (path.getFilePath() != null) {
            opts.setPath(path.getFilePath());
        }
        return path.getFileSystemClient().listPaths(opts, null).iterator();
    }

    @Override
    protected InputStream newInputStreamInternal(final OneLakePath path, final OpenOption... options)
            throws IOException {

        return new OneLakeInputStream(path);
    }

    @SuppressWarnings("resource")
    @Override
    protected OutputStream newOutputStreamInternal(final OneLakePath path, final OpenOption... options)
            throws IOException {

        final Set<OpenOption> opts = new HashSet<>(Arrays.asList(options));
        return Channels.newOutputStream(newByteChannel(path, opts));
    }

    @Override
    protected Iterator<OneLakePath> createPathIterator(final OneLakePath dir, final Filter<? super Path> filter)
            throws IOException {
        return OneLakePathIteratorFactory.create(dir, filter);
    }

    @Override
    protected void createDirectoryInternal(final OneLakePath dir, final FileAttribute<?>... attrs) throws IOException {
        if (dir.isManagedPath()) {
            throw new AccessDeniedException(dir.toString(), //
                    null, //
                    "cannot create folders in Fabric-managed locations");
        }

        final var fsClient = dir.getFileSystemClient();
        final var filePath = dir.getFilePath();

        try {
            if (filePath != null) {
                fsClient.createDirectory(filePath);
            } else {
                fsClient.create();
            }
        } catch (DataLakeStorageException ex) {
            throw AzureUtils.toIOE(ex, dir.toString());
        }
    }

    @Override
    protected BaseFileAttributes fetchAttributesInternal(final OneLakePath path, final Class<?> type)
            throws IOException {

        try {
            if (path.isRoot()) {
                return fetchAttributesForFileSystem(path);
            } else if (path.isManagedPath()) {
                return fetchManagedFolderAttributes(path);
            } else {
                return fetchAttributesForFile(path);
            }
        } catch (DataLakeStorageException ex) {
            throw AzureUtils.toIOE(ex, path.toString());
        }
    }

    @SuppressWarnings("resource")
    private BaseFileAttributes fetchManagedFolderAttributes(final OneLakePath path) throws IOException {

        var attributes = getFileSystemInternal().getManagedPathAttributes(path).orElse(null);

        if (attributes == null) {
            final var pathIter = OneLakePathIteratorFactory.create((OneLakePath) path.getParent(), p -> true);

            while (pathIter.hasNext()) {
                final var childPath = pathIter.next();
                if (childPath.equals(path)) {
                    attributes = getFileSystemInternal().getManagedPathAttributes(path).orElse(null);
                    break;
                }
            }
        }

        if (attributes == null) {
            throw new NoSuchFileException(path.toString());
        } else {
            return attributes;
        }
    }

    private static BaseFileAttributes fetchAttributesForFile(final OneLakePath path) {

        final var properties = path.getFileClient().getProperties();

        final var createdAt = Optional.of(properties)// NOSONAR it's not null
                .map(PathProperties::getCreationTime)//
                .map(t -> FileTime.from(t.toInstant()))//
                .orElse(FileTime.fromMillis(0));

        final var modifiedAt = Optional.of(properties)// NOSONAR it's not null
                .map(PathProperties::getLastModified)//
                .map(t -> FileTime.from(t.toInstant()))//
                .orElse(FileTime.fromMillis(0));

        return new BaseFileAttributes(//
                !properties.isDirectory(), // NOSONAR it's not null
                path, //
                modifiedAt, //
                modifiedAt, //
                createdAt, //
                properties.getFileSize(), //
                false, //
                false, //
                null);
    }

    private static BaseFileAttributes fetchAttributesForFileSystem(final OneLakePath path) {

        final var properties = path.getFileSystemClient().getProperties();

        final var lastModifiedTime = Optional.ofNullable(properties)//
                .map(FileSystemProperties::getLastModified)//
                .map(t -> FileTime.from(t.toInstant()))//
                .orElse(FileTime.fromMillis(0));

        return new BaseFileAttributes(//
                false, //
                path, //
                lastModifiedTime, //
                lastModifiedTime, //
                lastModifiedTime, //
                0, //
                false, //
                false, //
                null);
    }

    @Override
    protected void checkAccessInternal(final OneLakePath path, final AccessMode... modes) throws IOException {
        // nothing to do here
    }

    @Override
    protected void deleteInternal(final OneLakePath path) throws IOException {
        // we cannot read/write managed paths
        if (path.isManagedPath()) {
            throw new AccessDeniedException(path.toString(), //
                    null, //
                    "cannot delete Fabric-managed files/folders");
        }

        try {
            if (path.getFilePath() != null) {
                path.getFileClient().delete();
            } else {
                path.getFileSystemClient().delete();
            }
        } catch (DataLakeStorageException ex) {
            throw AzureUtils.toIOE(ex, path.toString());
        }
    }

}
