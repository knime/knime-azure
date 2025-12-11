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
package org.knime.ext.azure.blobstorage.filehandling.node;

import java.io.File;
import java.io.IOException;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.credentials.base.Credential;
import org.knime.credentials.base.CredentialPortObject;
import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.core.webui.node.dialog.defaultdialog.NodeParametersUtil;
import org.knime.ext.azure.AzureUtils;
import org.knime.ext.azure.blobstorage.filehandling.fs.AzureBlobStorageFSConnection;
import org.knime.ext.azure.blobstorage.filehandling.fs.AzureBlobStorageFSConnectionConfig;
import org.knime.filehandling.core.connections.FSConnectionRegistry;
import org.knime.filehandling.core.port.FileSystemPortObject;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;

/**
 * Azure Blob Storage Connector node model.
 *
 * @author Alexander Bondaletov
 */
@SuppressWarnings("restriction")
class AzureBlobStorageConnectorNodeModel extends NodeModel {

    private static final String FILE_SYSTEM_NAME = "Azure Blob Storage";

    private String m_fsId;
    private AzureBlobStorageFSConnection m_fsConnection;
    private AzureBlobStorageConnectorNodeParameters m_parameters = new AzureBlobStorageConnectorNodeParameters();

    /**
     * Creates new instance.
     */
    protected AzureBlobStorageConnectorNodeModel() {
        super(new PortType[] { CredentialPortObject.TYPE }, new PortType[] { FileSystemPortObject.TYPE });
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        m_fsId = FSConnectionRegistry.getInstance().getKey();
        var credential = ((CredentialPortObjectSpec) inSpecs[0]).getCredential(Credential.class);
        var spec = credential.map(this::createSpec).orElse(null);
        return new PortObjectSpec[] { spec };
    }

    private FileSystemPortObjectSpec createSpec(final Credential credential) {
        final String storageAccount = AzureUtils.getStorageAccount(credential);
        return new FileSystemPortObjectSpec(FILE_SYSTEM_NAME, m_fsId,
                AzureBlobStorageFSConnectionConfig.createFSLocationSpec(storageAccount));
    }

    @SuppressWarnings("resource")
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        var credential = ((CredentialPortObject) inObjects[0]).getCredential(Credential.class)
                .orElseThrow(() -> new InvalidSettingsException(
                        "Credential is not available. Please re-execute authenticator node."));

        final AzureBlobStorageFSConnectionConfig config = toFSConnectionConfig(m_parameters, credential);
        m_fsConnection = new AzureBlobStorageFSConnection(config);

        if (!m_fsConnection.getFileSystem().canCredentialsListContainers()) {
            setWarningMessage(
                    "Authentication failed, or the account doesn't have enough permissions to list containers");
        }
        FSConnectionRegistry.getInstance().register(m_fsId, m_fsConnection);

        return new PortObject[] { new FileSystemPortObject(createSpec(credential)) };
    }

    private static AzureBlobStorageFSConnectionConfig toFSConnectionConfig(
            final AzureBlobStorageConnectorNodeParameters settings, final Credential credential) {
        final AzureBlobStorageFSConnectionConfig config = new AzureBlobStorageFSConnectionConfig(
                settings.m_workingDirectory);
        config.setCredential(credential);
        config.setNormalizePaths(settings.m_normalizePaths);
        config.setTimeout(java.time.Duration.ofSeconds(settings.m_timeout));
        return config;
    }

    /**
     * Tests the connection by attempting to list containers. This is used by the file browser
     * to validate the connection before showing the file system.
     *
     * @param connection the connection to test
     * @throws IOException if the connection test fails
     */
    static void testConnection(final AzureBlobStorageFSConnection connection) throws IOException {
        // Test the connection by checking if we can list containers
        if (!connection.getFileSystem().canCredentialsListContainers()) {
            throw new IOException(
                    "Authentication failed, or the account doesn't have enough permissions to list containers");
        }
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
        setWarningMessage("Connection no longer available. Please re-execute the node.");
    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
        // nothing to save
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        NodeParametersUtil.saveSettings(AzureBlobStorageConnectorNodeParameters.class, m_parameters, settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        var parameters = NodeParametersUtil.loadSettings(settings, AzureBlobStorageConnectorNodeParameters.class);
        parameters.validate();
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_parameters = NodeParametersUtil.loadSettings(settings, AzureBlobStorageConnectorNodeParameters.class);
    }

    @Override
    protected void onDispose() {
        reset();
    }

    @Override
    protected void reset() {
        if (m_fsConnection != null) {
            m_fsConnection.closeInBackground();
            m_fsConnection = null;
        }
        m_fsId = null;
    }

}
