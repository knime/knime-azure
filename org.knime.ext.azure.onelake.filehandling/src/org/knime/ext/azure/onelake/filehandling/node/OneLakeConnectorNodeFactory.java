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
            <p>Connects to the OneLake data of a Microsoft Fabric workspace, in order to read/write files
            in downstream nodes.
            </p>

            <p>
            The resulting output port allows downstream nodes to access OneLake as a file system, e.g.
            to read or write files and folders, or to perform other file system operations (browse/list
            files, copy, move, ...).
            </p>

            <p>
            <b>Path syntax:</b> Paths for this connector are specified with a UNIX-like syntax, for example
            <tt>/Example.Lakehouse/Files/myfolder/file.csv</tt>, which is an absolute
            path that consists of:
            <ol>
                <li>a leading slash (<tt>/</tt>)</li>
                <li>the name of a Fabric-managed item in the workspace (<tt>Example.Lakehouse</tt>)</li>
                <li>the name of a Fabric-managed folder inside the item (<tt>Files</tt>)</li>
                <li>The name of a folder or file (<tt>myfolder/file.csv</tt>)</li>
            </ol>
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
