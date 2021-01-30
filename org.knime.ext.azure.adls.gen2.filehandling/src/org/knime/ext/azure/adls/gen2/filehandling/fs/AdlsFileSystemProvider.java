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
 *   2020-12-17 (Alexander Bondaletov): created
 */
package org.knime.ext.azure.adls.gen2.filehandling.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.knime.filehandling.core.connections.base.BaseFileSystemProvider;
import org.knime.filehandling.core.connections.base.attributes.BaseFileAttributes;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.models.FileSystemProperties;
import com.azure.storage.file.datalake.models.PathProperties;

/**
 * File system provider for the {@link AdlsFileSystem}.
 *
 * @author Alexander Bondaletov
 */
public class AdlsFileSystemProvider extends BaseFileSystemProvider<AdlsPath, AdlsFileSystem> {
    /**
     * ADLS URI scheme.
     */
    public static final String FS_TYPE = "microsoft-adlsgen2";

    @Override
    protected SeekableByteChannel newByteChannelInternal(final AdlsPath path, final Set<? extends OpenOption> options,
            final FileAttribute<?>... attrs) throws IOException {

        // we cannot read/write a file below the root (only containers can be there)
        if (path.getParent() != null && ((AdlsPath) path.getParent()).isRoot()) {
            throw new IOException("Cannot read/write files below the root. Only folders can be there.");
        }

        return new AdlsSeekableByteChannel(path, options);
    }

    @Override
    protected void copyInternal(final AdlsPath source, final AdlsPath target, final CopyOption... options) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    protected InputStream newInputStreamInternal(final AdlsPath path, final OpenOption... options) throws IOException {
        return new AdlsInputStream(path);
    }

    @SuppressWarnings("resource")
    @Override
    protected OutputStream newOutputStreamInternal(final AdlsPath path, final OpenOption... options) throws IOException {
        final Set<OpenOption> opts = new HashSet<>(Arrays.asList(options));
        return Channels.newOutputStream(newByteChannel(path, opts));
    }

    @Override
    protected Iterator<AdlsPath> createPathIterator(final AdlsPath dir, final Filter<? super Path> filter) throws IOException {
        return AdlsPathIteratorFactory.create(dir, filter);
    }

    @Override
    protected void createDirectoryInternal(final AdlsPath dir, final FileAttribute<?>... attrs) throws IOException {
        DataLakeFileSystemClient fsClient = dir.getFileSystemClient();
        String filePath = dir.getFilePath();

        if (filePath != null) {
            fsClient.createDirectory(filePath);
        } else {
            fsClient.create();
        }
    }

    @Override
    protected BaseFileAttributes fetchAttributesInternal(final AdlsPath path, final Class<?> type) throws IOException {
        if (path.isRoot()) {
            return createFakeRootAttributes(path);
        } else if (path.getFilePath() != null) {
            return fetchAttributesForFile(path);
        } else {
            return fetchAttributesForFileSystem(path);
        }
    }

    private static BaseFileAttributes createFakeRootAttributes(final AdlsPath path) {
        FileTime time = FileTime.fromMillis(0);
        return new BaseFileAttributes(false, path, time, time, time, 0, false, false, null);
    }

    private static BaseFileAttributes fetchAttributesForFile(final AdlsPath path) {
        DataLakeFileClient fileClient = path.getFileClient();
        PathProperties properties = fileClient.getProperties();

        FileTime createdAt = FileTime.from(properties.getCreationTime().toInstant());
        FileTime modifiedAt = FileTime.from(properties.getLastModified().toInstant());

        return new BaseFileAttributes(!properties.isDirectory(), path, modifiedAt, modifiedAt, createdAt,
                properties.getFileSize(), false, false, null);
    }

    private static BaseFileAttributes fetchAttributesForFileSystem(final AdlsPath path) {
        FileSystemProperties properties = path.getFileSystemClient().getProperties();

        FileTime modifiedAt = FileTime.from(properties.getLastModified().toInstant());

        return new BaseFileAttributes(false, path, modifiedAt, modifiedAt, modifiedAt, 0, false, false, null);
    }

    @Override
    protected boolean exists(final AdlsPath path) throws IOException {
        if (path.getFilePath() != null) {
            return path.getFileClient().exists();
        } else {
            return super.exists(path);
        }
    }

    @Override
    protected void checkAccessInternal(final AdlsPath path, final AccessMode... modes) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    protected void deleteInternal(final AdlsPath path) throws IOException {
        // TODO Auto-generated method stub

    }

}
