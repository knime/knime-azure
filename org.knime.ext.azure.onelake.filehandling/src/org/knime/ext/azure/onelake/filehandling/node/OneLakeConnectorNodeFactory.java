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

import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeFactory;
import org.knime.ext.azure.fabric.port.FabricWorkspacePortObject;
import org.knime.filehandling.core.port.FileSystemPortObject;

/**
 * Node factory for the Microsoft OneLake File System Connector.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
@SuppressWarnings("restriction")
public class OneLakeConnectorNodeFactory extends WebUINodeFactory<OneLakeConnectorNodeModel> {

    private static final String FULL_DESCRIPTION = """
            <p>
            Establishes a connection to the
            <a href="https://learn.microsoft.com/en-us/fabric/onelake/onelake-overview">OneLake</a> storage of a
            <a href="https://learn.microsoft.com/en-us/fabric/fundamentals/microsoft-fabric-overview">Microsoft Fabric workspace,</a>
            enabling downstream nodes to access, read, and write files.
            </p>

            <p>
            The node's output port exposes OneLake as a
            <a href="https://docs.knime.com/latest/analytics_platform_file_handling_guide/index.html#connected-fs">connected file system.</a>
            This allows downstream nodes to perform common file operations, including reading, writing, browsing,
            listing, copying, and moving files and folders.
            For an overview of working with files and file systems in KNIME, see the
            <a href="https://docs.knime.com/latest/analytics_platform_file_handling_guide/index.html">KNIME File Handling Guide.</a>.
            </p>

            <p>
            <b>Path syntax</b> <br/>
            Paths use a UNIX-like format. For example: <tt>/Example.Lakehouse/Files/myfolder/file.csv</tt>.
            This absolute path includes:
            <ol>
                <li>Aa leading slash (<tt>/</tt>)</li>
                <li>The name of a Fabric-managed item in the workspace (<tt>Example.Lakehouse</tt>)</li>
                <li>A predefined folder within the item (<tt>Files</tt>)</li>
                <li>A user-defined folder and file (<tt>myfolder/file.csv</tt>)</li>
            </ol>
            </p>

            <p>
            <b>Limitations</b> <br/>
            <ul>
                <li>
                    Fabric-managed paths are <b>read-only</b>. These include top-level items such as
                    <tt>/MyLakehouse.lakehouse</tt> and their system folders like <tt>/MyLakehouse.lakehouse/Files</tt>
                    and <tt>/MyLakehouse.lakehouse/Tables</tt>.
                    <br/>
                    The <b>Tables</b> folder contains
                    <a href="https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-and-delta-tables">Delta Lake files</a>
                    for Fabric-managed tables and must not be modified directly.
                    <br/>
                    Within the <b>Files</b> folder, users can freely create, modify, move, or delete their own
                    subfolders and files.
                </li>
                <li>
                    <a href="https://learn.microsoft.com/en-us/fabric/fundamentals/workspaces-folders"><b>Workspace folders</b></a>
                    are not represented in the file system. Their contents appear at the root level with unique names.
                    This behavior is due to current limitations of the
                    <a href="https://learn.microsoft.com/en-us/rest/api/fabric/core/folders">OneLake API,</a>
                    which is in preview and subject to change.
                </li>
            </ul>
            </p>
            """;

    private static final WebUINodeConfiguration CONFIGURATION = WebUINodeConfiguration.builder()//
            .name("Microsoft OneLake Connector") //
            .icon("./file_system_connector.png") //
            .shortDescription("Microsoft OneLake Connector node.") //
            .fullDescription(FULL_DESCRIPTION) //
            .modelSettingsClass(OneLakeConnectorSettings.class) //
            .nodeType(NodeType.Source)//
            .addInputPort("Microsoft Fabric Workspace Connection", //
                    FabricWorkspacePortObject.TYPE, //
                    "Microsoft Fabric Workspace Connection") //
            .addOutputPort("File System", FileSystemPortObject.TYPE, "Microsoft OneLake File System.") //
            .sinceVersion(5, 3, 0)//
            .build();

    /**
     * Creates new instance.
     */
    public OneLakeConnectorNodeFactory() {
        super(CONFIGURATION);
    }

    @Override
    public OneLakeConnectorNodeModel createNodeModel() {
        return new OneLakeConnectorNodeModel(CONFIGURATION);
    }
}
