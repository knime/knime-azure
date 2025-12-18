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

import static org.knime.node.impl.description.PortDescription.fixedPort;

import java.util.List;
import java.util.Map;

import org.knime.core.node.NodeDescription;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory;
import org.knime.core.node.NodeView;
import org.knime.core.webui.node.dialog.NodeDialog;
import org.knime.core.webui.node.dialog.NodeDialogFactory;
import org.knime.core.webui.node.dialog.NodeDialogManager;
import org.knime.core.webui.node.dialog.SettingsType;
import org.knime.core.webui.node.dialog.defaultdialog.DefaultKaiNodeInterface;
import org.knime.core.webui.node.dialog.defaultdialog.DefaultNodeDialog;
import org.knime.core.webui.node.dialog.kai.KaiNodeInterface;
import org.knime.core.webui.node.dialog.kai.KaiNodeInterfaceFactory;
import org.knime.node.impl.description.DefaultNodeDescriptionUtil;
import org.knime.node.impl.description.PortDescription;

/**
 * Factory class for the Azure Blob Storage Connector node.
 *
 * @author Alexander Bondaletov
 */
@SuppressWarnings("restriction")
public class AzureBlobStorageConnectorNodeFactory extends NodeFactory<AzureBlobStorageConnectorNodeModel>
        implements NodeDialogFactory, KaiNodeInterfaceFactory {

    @Override
    public AzureBlobStorageConnectorNodeModel createNodeModel() {
        return new AzureBlobStorageConnectorNodeModel();
    }

    @Override
    protected int getNrNodeViews() {
        return 0;
    }

    @Override
    public NodeView<AzureBlobStorageConnectorNodeModel> createNodeView(final int viewIndex,
            final AzureBlobStorageConnectorNodeModel nodeModel) {
        return null;
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }

    private static final String NODE_NAME = "Azure Blob Storage Connector";
    private static final String NODE_ICON = "./file_system_connector.png";
    private static final String SHORT_DESCRIPTION = """
            Connects to Azure Blob Storage in order to read/write files in downstream nodes.
            """;
    private static final String FULL_DESCRIPTION = """
            <p>This node connects to Azure Blob Storage. The resulting output port allows downstream nodes to access
            the Azure Blob Storage data as a file system, e.g. to read or write files and folders,
            or to perform other file system operations (browse/list files, copy, move, ...).
            </p>
            
            <p>This node requires the <i>Microsoft Authenticator</i> to perform authentication.</p>
            
            <p><b>Path syntax:</b> Paths for Azure Blob Storage are specified with a UNIX-like syntax,
            <tt>/mycontainer/myfolder/myfile</tt>. An absolute path for Azure Blob Storage consists of:
                <ol>
                    <li>A leading slash (<tt>/</tt>).</li>
                    <li>Followed by the name of a container (<tt>mycontainer</tt> in the above example), followed by a slash.</li>
                    <li>Followed by the name of an object within the container (<tt>myfolder/myfile</tt> in the above example).</li>
                </ol>
            </p>
            
            <p><b>URI formats:</b> When you apply the <i>Path to URI</i> node to paths coming from this connector, you
            can create URIs with the following formats:
                <ol>
                    <li><b>Shared Access Signature (SAS) URLs</b> which contain credentials, that allow to access files
            for a certain amount of time
                    (see <a href="https://docs.microsoft.com/en-us/azure/storage/common/storage-sas-overview">Azure
            documentation</a>).</li>
                    <li><b>wasbs:// URLs</b> to access Azure Blob Storage from inside Hadoop environments.</li>
                </ol>
            </p>
            """;
    private static final List<PortDescription> INPUT_PORTS = List.of(
            fixedPort("Credential", """
                    Attach the Microsoft Authenticator node to perform authentication and provide a credential.
                    """)
    );
    private static final List<PortDescription> OUTPUT_PORTS = List.of(
            fixedPort("Azure Blob Storage File System Connection", """
                    Azure Blob Storage File System Connection
                    """)
    );

    @Override
    protected NodeDialogPane createNodeDialogPane() {
        return NodeDialogManager.createLegacyFlowVariableNodeDialog(createNodeDialog());
    }

    @Override
    public NodeDialog createNodeDialog() {
        return new DefaultNodeDialog(SettingsType.MODEL, AzureBlobStorageConnectorNodeParameters.class);
    }

    @Override
    public NodeDescription createNodeDescription() {
        return DefaultNodeDescriptionUtil.createNodeDescription(
                NODE_NAME,
                NODE_ICON,
                INPUT_PORTS,
                OUTPUT_PORTS,
                SHORT_DESCRIPTION,
                FULL_DESCRIPTION,
                List.of(),
                AzureBlobStorageConnectorNodeParameters.class,
                null,
                NodeType.Source,
                List.of(),
                null
        );
    }

    @Override
    public KaiNodeInterface createKaiNodeInterface() {
        return new DefaultKaiNodeInterface(Map.of(SettingsType.MODEL, AzureBlobStorageConnectorNodeParameters.class));
    }
}
