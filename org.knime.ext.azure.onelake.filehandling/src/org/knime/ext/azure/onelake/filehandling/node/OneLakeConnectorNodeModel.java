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
 *   2024-05-24 (Sascha Wolke, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.ext.azure.onelake.filehandling.node;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;

import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.KNIMEException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeModel;
import org.knime.credentials.base.Credential;
import org.knime.credentials.base.NoSuchCredentialException;
import org.knime.ext.azure.fabric.port.FabricConnection;
import org.knime.ext.azure.fabric.port.FabricWorkspacePortObjectSpec;
import org.knime.ext.azure.fabric.rest.FabricRESTClient;
import org.knime.ext.azure.fabric.rest.workspace.WorkspaceAPI;
import org.knime.ext.azure.onelake.filehandling.fs.OneLakeFSConnection;
import org.knime.ext.azure.onelake.filehandling.fs.OneLakeFSConnectionConfig;
import org.knime.ext.azure.onelake.filehandling.fs.OneLakeFSDescriptorProvider;
import org.knime.filehandling.core.connections.FSConnectionRegistry;
import org.knime.filehandling.core.port.FileSystemPortObject;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;

import jakarta.ws.rs.WebApplicationException;

/**
 * Node model for the Microsoft OneLake Connector.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SuppressWarnings("restriction")
public class OneLakeConnectorNodeModel extends WebUINodeModel<OneLakeConnectorSettings> {

    private String m_fsId;
    private OneLakeFSConnection m_fsConnection;

    OneLakeConnectorNodeModel(final WebUINodeConfiguration configuration) {
        super(configuration, OneLakeConnectorSettings.class);
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs,
        final OneLakeConnectorSettings settings)
        throws InvalidSettingsException {

        m_fsId = FSConnectionRegistry.getInstance().getKey();

        if (inSpecs[0] == null) {
            return new PortObjectSpec[] { null };
        }

        final var fabricConnection = ((FabricWorkspacePortObjectSpec) inSpecs[0]).getFabricConnection();
        try {
            fabricConnection.getCredential().resolveCredential(Credential.class);
        } catch (NoSuchCredentialException ex) {
            throw new InvalidSettingsException(ex.getMessage(), ex);
        }

        // cannot create a file system spec without a workspace name, which needs to be
        // resolved during execution
        return new PortObjectSpec[] { createSpec(fabricConnection.getWorkspaceId()) };
    }


    private FileSystemPortObjectSpec createSpec(final String workspaceId) {

        return new FileSystemPortObjectSpec(//
                OneLakeFSDescriptorProvider.FS_TYPE.getTypeId(), //
                m_fsId, //
                OneLakeFSConnectionConfig.createFSLocationSpec(workspaceId));
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec,
        final OneLakeConnectorSettings settings) throws Exception {

        final var fabricConnection = ((FabricWorkspacePortObjectSpec) inObjects[0].getSpec()).getFabricConnection();

        final var fabricWorkspaceName = resolveFabricWorkspaceName(fabricConnection);

        final var config = settings.createFSConnectionConfig(fabricConnection, fabricWorkspaceName);

        m_fsConnection = new OneLakeFSConnection(config);
        FSConnectionRegistry.getInstance().register(m_fsId, m_fsConnection);

        testConnection(m_fsConnection);

        return new PortObject[] { new FileSystemPortObject(createSpec(fabricConnection.getWorkspaceId())) };
    }

    private String resolveFabricWorkspaceName(final FabricConnection fabricConnection)
            throws NoSuchCredentialException, KNIMEException {

        try {
            final var client = FabricRESTClient.fromFabricConnection(WorkspaceAPI.class, //
                    fabricConnection);

            final var workspace = client.getWorkspace(fabricConnection.getWorkspaceId());

            return workspace.displayName;
        } catch (IOException | WebApplicationException e) {
            throw createUnableToAccessWorkspaceException(e);
        }
    }

    private KNIMEException createUnableToAccessWorkspaceException(final Exception e) {
        return KNIMEException.of(//
                createMessageBuilder()//
                        .withSummary("Unable to access the selected Fabric workspace.")//
                        .addResolutions(
                                """
                                        Check that the Fabric workspace selected in the preceding 'Microsoft Fabric
                                        Workspace Connector' node exists.
                                        """)//
                        .addResolutions(
                                        """
                                        Check that the current user has access to the Fabric workspace.
                                        """)//
                        .build()//
                        .orElseThrow(), //
                e);
    }

    @SuppressWarnings("resource")
    private void testConnection(final OneLakeFSConnection connection) throws KNIMEException {
        try {
            final var workingDir = connection.getFileSystem().getWorkingDirectory();

            Files.list(workingDir).findFirst().orElse(null); // NOSONAR method has a side effect

        } catch (IOException | UncheckedIOException e) {
            connection.closeInBackground();

            Exception realEx = e;
            if (e instanceof UncheckedIOException uncheckedEx) {
                realEx = uncheckedEx.getCause();
            }

            throw createUnableToAccessWorkspaceException(realEx);
        }
    }

    @Override
    protected void onDispose() {
        // close the file system also when the workflow is closed
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
