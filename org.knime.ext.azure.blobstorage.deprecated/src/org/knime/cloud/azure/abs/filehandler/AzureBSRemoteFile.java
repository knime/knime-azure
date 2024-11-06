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
 *   Aug 11, 2016 (oole): created
 */
package org.knime.cloud.azure.abs.filehandler;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.cloud.core.file.CloudRemoteFile;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.util.CheckUtils;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobItem;

/**
 * Implementation of {@link CloudRemoteFile} for Azure Blob Store
 *
 * @author Ole Ostergaard, KNIME.com GmbH
 */
public class AzureBSRemoteFile extends CloudRemoteFile<AzureBSConnection> {

	/**
	 * @param uri
	 * @param connectionInformation
	 * @param connectionMonitor
	 */
	public AzureBSRemoteFile(final URI uri, final CloudConnectionInformation connectionInformation,
			final ConnectionMonitor<AzureBSConnection> connectionMonitor) {
		this(uri, connectionInformation, connectionMonitor, null, null);
	}

	/**
	 * @param uri
	 * @param connectionInformation
	 * @param connectionMonitor
	 * @param containerName
	 * @param blob
	 */
	public AzureBSRemoteFile(final URI uri, final CloudConnectionInformation connectionInformation,
			final ConnectionMonitor<AzureBSConnection> connectionMonitor, final String containerName, final BlobItem blob) {
		super(uri, connectionInformation, connectionMonitor);
		CheckUtils.checkArgumentNotNull(connectionInformation, "Connection Information must not be null");
		if (blob != null) {
			m_containerName = containerName;
			m_blobName = blob.getName();

			if (!blob.isPrefix().booleanValue()) { // directory
				m_lastModified = blob.getProperties().getLastModified().toEpochSecond();
				m_size = blob.getProperties().getContentLength();
			}
		}
	}

	private BlobServiceClient getClient() throws Exception {
		return getOpenedConnection().getClient();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected AzureBSConnection createConnection() {
		return new AzureBSConnection((CloudConnectionInformation)getConnectionInformation());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean doesContainerExist(final String containerName) throws Exception {
		return getClient().getBlobContainerClient(containerName).exists();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean doestBlobExist(final String containerName, final String blobName) throws Exception {
		return getClient().getBlobContainerClient(containerName).getBlobClient(blobName).exists();
	}

	@Override
	protected AzureBSRemoteFile[] listRootFiles() throws Exception {
		final PagedIterable<BlobContainerItem> containers = getClient().listBlobContainers();
		final List<BlobContainerItem> containerList = containers.stream().collect(Collectors.toList());
		final AzureBSRemoteFile[] files = new AzureBSRemoteFile[containerList.size()];
		for (int i = 0; i < files.length; i++) {
			final URI uri = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(),
					getURI().getPort(), createContainerPath(containerList.get(i).getName()), getURI().getQuery(),
					getURI().getFragment());
			files[i] = new AzureBSRemoteFile(uri, (CloudConnectionInformation)getConnectionInformation(),
					getConnectionMonitor());
		}
		return files;
	}

	@Override
	protected AzureBSRemoteFile[] listDirectoryFiles() throws Exception {
		final String containerName = getContainerName();
		final String prefix = getBlobName();
		final PagedIterable<BlobItem> blobs = getClient().getBlobContainerClient(containerName).listBlobsByHierarchy(prefix);
		final Iterator<BlobItem> iterator = blobs.iterator();

		final List<AzureBSRemoteFile> fileList = new ArrayList<AzureBSRemoteFile>();
		while (iterator.hasNext()) {
			final BlobItem blob = iterator.next();
			if (!blob.getName().equals(prefix)) {
				final URI uri = new URI(getURI().getScheme(), getURI().getUserInfo(), getURI().getHost(),
						getURI().getPort(), createContainerPath(containerName) + blob.getName(),
						getURI().getQuery(), getURI().getFragment());
				fileList.add(new AzureBSRemoteFile(uri, (CloudConnectionInformation)getConnectionInformation(),
							getConnectionMonitor(), containerName, blob));
			}
		}
		final AzureBSRemoteFile[] files = fileList.toArray(new AzureBSRemoteFile[fileList.size()]);
		return files;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected long getBlobSize() throws Exception {
		return getClient().getBlobContainerClient(getContainerName()).getBlobClient(getBlobName()).getProperties()
			.getBlobSize();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected long getLastModified() throws Exception {
		return getClient().getBlobContainerClient(getContainerName()).getBlobClient(getBlobName()).getProperties()
			.getLastModified().toEpochSecond();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean deleteContainer() throws Exception {
		return getClient().getBlobContainerClient(getContainerName()).deleteIfExists();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean deleteDirectory() throws Exception {
		boolean result = exists();
		final var container = getClient().getBlobContainerClient(getContainerName());
		final PagedIterable<BlobItem> blobs = container.listBlobsByHierarchy(getBlobName());
		final Iterator<BlobItem> iterator = blobs.iterator();
		while (iterator.hasNext()) {
			final BlobItem next = iterator.next();
			result = container.getBlobClient(next.getName()).deleteIfExists();
		}
		return result;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean deleteBlob() throws Exception {
		return getClient().getBlobContainerClient(getContainerName()).getBlobClient(getBlobName()).deleteIfExists();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean createContainer() throws Exception {
		return getClient().getBlobContainerClient(getContainerName()).createIfNotExists();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean createDirectory(final String dirName) throws Exception {
		final BlobClient dir = getClient().getBlobContainerClient(getContainerName()).getBlobClient(dirName);
		final InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

		dir.upload(emptyContent, 0l);
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputStream openInputStream() throws Exception {
        return getClient() //
            .getBlobContainerClient(getContainerName()) //
            .getBlobClient(getBlobName()) //
            .openInputStream();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public OutputStream openOutputStream() throws Exception {
		return getClient().getBlobContainerClient(getContainerName()) //
			.getBlobClient(getBlobName()) //
			.getBlockBlobClient() //
			.getBlobOutputStream();
	}

    @Override
    public URI getHadoopFilesystemURI() throws Exception {
        // wasb(s)://<containername>@<accountname>.blob.core.windows.net/path
        final CloudConnectionInformation connectionInfo = (CloudConnectionInformation) getConnectionInformation();
        final String scheme = "wasb";
        final String account = connectionInfo.getUser();
        final String container = getContainerName();
        final String host = account + ".blob.core.windows.net";
        final String blobName = Optional.ofNullable(getBlobName()).orElseGet(() -> "");
        return new URI(scheme, container, host, -1, DELIMITER + blobName, null, null);
    }
}
