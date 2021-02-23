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
 *   2020-12-16 (Alexander Bondaletov): created
 */
package org.knime.ext.azure.adls.gen2.filehandling.node;

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
import org.knime.ext.azure.AzureUtils;
import org.knime.ext.azure.OAuthTokenCredential;
import org.knime.ext.azure.adls.gen2.filehandling.fs.AdlsFSConnection;
import org.knime.ext.azure.adls.gen2.filehandling.fs.AdlsFileSystem;
import org.knime.ext.microsoft.authentication.port.MicrosoftCredential;
import org.knime.ext.microsoft.authentication.port.MicrosoftCredentialPortObject;
import org.knime.ext.microsoft.authentication.port.MicrosoftCredentialPortObjectSpec;
import org.knime.ext.microsoft.authentication.port.azure.storage.AzureSharedKeyCredential;
import org.knime.ext.microsoft.authentication.port.oauth2.OAuth2Credential;
import org.knime.filehandling.core.connections.FSConnectionRegistry;
import org.knime.filehandling.core.port.FileSystemPortObject;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;

import com.azure.core.http.policy.TimeoutPolicy;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.DataLakeStorageException;

/**
 * ADLS Gen2 Connector node.
 *
 * @author Alexander Bondaletov
 */
public class AdlsConnectorNodeModel extends NodeModel {
    private static final String FILE_SYSTEM_NAME = "Azure Data Lake Storage Gen2";

    private String m_fsId;
    private AdlsFSConnection m_fsConnection;

    private final AdlsConnectorSettings m_settings = new AdlsConnectorSettings();

    /**
     * Creates new instance.
     */
    protected AdlsConnectorNodeModel() {
        super(new PortType[] { MicrosoftCredentialPortObject.TYPE }, new PortType[] { FileSystemPortObject.TYPE });
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        MicrosoftCredential credential = ((MicrosoftCredentialPortObjectSpec) inSpecs[0]).getMicrosoftCredential();
        if (credential == null) {
            throw new InvalidSettingsException("Not authenticated");
        }

        m_fsId = FSConnectionRegistry.getInstance().getKey();

        return new PortObjectSpec[] { createSpec(credential) };
    }

    private FileSystemPortObjectSpec createSpec(final MicrosoftCredential credential) {
        String storageAccount = AzureUtils.getStorageAccount(credential);
        return new FileSystemPortObjectSpec(FILE_SYSTEM_NAME, m_fsId,
                AdlsFileSystem.createFSLocationSpec(storageAccount));
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        MicrosoftCredential credential = ((MicrosoftCredentialPortObject) inObjects[0]).getMicrosoftCredentials();
        DataLakeServiceClient client = createClient(credential, m_settings);
        try {
            // initialize lazy iterator by calling haxNext to make list containers request
            client.listFileSystems().iterator().hasNext();// NOSONAR
        } catch (DataLakeStorageException ex) {
            AzureUtils.handleAuthException(ex);
            setWarningMessage(
                    "Authentication failed, or the account doesn't have enough permissions to list containers");
        }

        m_fsConnection = new AdlsFSConnection(client, m_settings.getWorkingDirectory());
        FSConnectionRegistry.getInstance().register(m_fsId, m_fsConnection);

        return new PortObject[] { new FileSystemPortObject(createSpec(credential)) };
    }

    static DataLakeServiceClient createClient(final MicrosoftCredential credential,
            final AdlsConnectorSettings settings) throws IOException {
        DataLakeServiceClientBuilder builder = new DataLakeServiceClientBuilder()
                .endpoint(AzureUtils.getEndpoint(credential))//
                .addPolicy(new TimeoutPolicy(settings.getTimeout()));

        switch (credential.getType()) {
        case AZURE_SHARED_KEY:
            AzureSharedKeyCredential c = (AzureSharedKeyCredential) credential;
            builder.credential(new StorageSharedKeyCredential(c.getAccount(), c.getSecretKey()));
            break;
        case AZURE_SAS_TOKEN:
            // SAS token is a part of the endpoint
            break;
        case OAUTH2_ACCESS_TOKEN:
            final OAuth2Credential oauth2Credential = (OAuth2Credential) credential;
            builder.credential(new OAuthTokenCredential(oauth2Credential.getAccessToken()));
            break;
        default:
            throw new UnsupportedOperationException("Unsupported credential type " + credential.getType());
        }

        return builder.buildClient();
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
        m_settings.saveSettingsTo(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
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
