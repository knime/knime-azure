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

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FSConnectionProvider;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FileSelectionWidget;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.SingleFileSelectionMode;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.WithCustomFileSystem;
import org.knime.credentials.base.NoSuchCredentialException;
import org.knime.ext.azure.fabric.port.FabricConnection;
import org.knime.ext.azure.fabric.port.FabricWorkspacePortObject;
import org.knime.ext.azure.fabric.rest.FabricRESTClient;
import org.knime.ext.azure.fabric.rest.workspace.WorkspaceAPI;
import org.knime.ext.azure.onelake.filehandling.fs.OneLakeFSConnection;
import org.knime.ext.azure.onelake.filehandling.fs.OneLakeFSConnectionConfig;
import org.knime.ext.azure.onelake.filehandling.fs.OneLakeFileSystem;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.updates.StateProvider;

import jakarta.ws.rs.WebApplicationException;

/**
 * Node settings for the Microsoft OneLake Connector.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SuppressWarnings("restriction")
public class OneLakeConnectorSettings implements NodeParameters {

    @Section(title = "File System")
    interface FileSystemSection {
    }

    @Widget(title = "Working directory", //
            description = "Specifies the <i>working directory</i> of the resulting file system connection."
                    + " The working directory must be specified as an absolute path."
                    + " A working directory allows downstream nodes to access files/folders using"
                    + " <i>relative</i> paths, i.e. paths that do not have a leading slash."
                    + " If not specified, the default working directory is \"/\".")
    @FileSelectionWidget(SingleFileSelectionMode.FOLDER)
    @WithCustomFileSystem(connectionProvider = FileSystemConnectionProvider.class)
    @Layout(FileSystemSection.class)
    String m_workingDirectory = OneLakeFileSystem.PATH_SEPARATOR;

    @Override
    public void validate() throws InvalidSettingsException {
        if (StringUtils.isAllBlank(m_workingDirectory)) {
            throw new InvalidSettingsException("Please specify a working directory.");
        } else if (!m_workingDirectory.startsWith(OneLakeFileSystem.PATH_SEPARATOR)) {
            throw new InvalidSettingsException("Working directory must be an absolute path that starts with \"/\"");
        }
    }

    OneLakeFSConnectionConfig createFSConnectionConfig(final FabricConnection fabricConnection,
            final String fabricWorkspaceName) throws IOException, NoSuchCredentialException {
        return createFSConnectionConfig(fabricConnection, fabricWorkspaceName, m_workingDirectory);
    }

    private static OneLakeFSConnectionConfig createFSConnectionConfig(final FabricConnection fabricConnection,
            final String fabricWorkspaceName, final String workingDirectory)
            throws IOException, NoSuchCredentialException {

        final var fsConfig = new OneLakeFSConnectionConfig(//
                fabricWorkspaceName, //
                fabricConnection.getWorkspaceId(), //
                workingDirectory);

        final var accessToken = OneLakeCredentialUtil.toAccessTokenAccessor(fabricConnection.getCredential());
        fsConfig.setAccessTokenAccessor(accessToken);

        fsConfig.setConnectionTimeout(fabricConnection.getConnectionTimeout());
        fsConfig.setReadTimeout(fabricConnection.getReadTimeout());

        return fsConfig;
    }

    static final class FileSystemConnectionProvider implements StateProvider<FSConnectionProvider> {

        private static final String ERROR_MSG = """
                Fabric workspace connection not available. Please re-execute the preceding connector node
                and make sure it is connected.
                """;

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeAfterOpenDialog();
        }

        @Override
        public FSConnectionProvider computeState(final NodeParametersInput parametersInput) {
            return () -> {
                final var fabricConnection = getFabricConnection(parametersInput);
                final var connection = createConnection(fabricConnection);
                testConnection(connection);
                return connection;
            };
        }

        private static FabricConnection getFabricConnection(final NodeParametersInput parametersInput)
                throws InvalidSettingsException {
            return parametersInput.getInPortObject(0) //
                    .filter(FabricWorkspacePortObject.class::isInstance) //
                    .map(FabricWorkspacePortObject.class::cast) //
                    .map(FabricWorkspacePortObject::getFabricConnection) //
                    .orElseThrow(() -> new InvalidSettingsException(ERROR_MSG));
        }

        private static OneLakeFSConnection createConnection(final FabricConnection fabricConnection)
                throws InvalidSettingsException {
            try {
                final var workspaceName = resolveWorkspaceName(fabricConnection);
                final var config = createFSConnectionConfig(fabricConnection, workspaceName,
                        OneLakeFileSystem.PATH_SEPARATOR);
                return new OneLakeFSConnection(config);
            } catch (IOException e) {
                throw new InvalidSettingsException("Unable to create the OneLake connection.", e);
            } catch (NoSuchCredentialException e) {
                throw new InvalidSettingsException("Selected Fabric credentials are not available.", e);
            }
        }

        private static String resolveWorkspaceName(final FabricConnection fabricConnection)
                throws InvalidSettingsException {
            try {
                final var client = FabricRESTClient.fromFabricConnection(WorkspaceAPI.class, fabricConnection);
                final var workspace = client.getWorkspace(fabricConnection.getWorkspaceId());
                return workspace.displayName;
            } catch (IOException | WebApplicationException | NoSuchCredentialException e) {
                throw new InvalidSettingsException("Unable to access the selected Fabric workspace.", e);
            }
        }

        @SuppressWarnings("resource")
        private static void testConnection(final OneLakeFSConnection connection) throws InvalidSettingsException {
            try {
                final var workingDir = connection.getFileSystem().getWorkingDirectory();
                Files.list(workingDir).findFirst();
            } catch (IOException | UncheckedIOException e) {
                connection.closeInBackground();
                throw new InvalidSettingsException("Unable to access the selected Fabric workspace.", e);
            }
        }
    }
}
