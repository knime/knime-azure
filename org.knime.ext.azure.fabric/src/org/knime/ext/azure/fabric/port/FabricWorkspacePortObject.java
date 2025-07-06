/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 *
 * History
 *   Aug 21, 2017 (oole): created
 */
package org.knime.ext.azure.fabric.port;

import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;
import org.knime.core.node.port.AbstractSimplePortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.PortTypeRegistry;
import org.knime.core.node.util.ViewUtils;

/**
 * Microsoft Fabric port object containing a Microsoft credential and workspace
 * info. It can be used to obtain a {@link FabricConnection}.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public final class FabricWorkspacePortObject extends AbstractSimplePortObject {

    /** The Serializer **/
    public static final class Serializer
        extends AbstractSimplePortObjectSerializer<FabricWorkspacePortObject> { }

    private FabricWorkspacePortObjectSpec m_spec;

    /**
     * The type of this port.
     */
    @SuppressWarnings("hiding")
    public static final PortType TYPE =
        PortTypeRegistry.getInstance().getPortType(FabricWorkspacePortObject.class);

    /**
     * Constructor used by the framework.
     */
    public FabricWorkspacePortObject() {
        // used by the framework
    }

    /**
     * Creates a new port object.
     *
     * @param spec
     *            The specification of this port object.
     */
    public FabricWorkspacePortObject(final FabricWorkspacePortObjectSpec spec) {
        m_spec = spec;
    }

    /**
     * @return The contained {@link FabricConnection} object
     */
    public FabricConnection getFabricConnection() {
        return m_spec.getFabricConnection();
    }

    @Override
    public String getSummary() {
        return m_spec.getFabricConnection().toString();
    }

    @Override
    public FabricWorkspacePortObjectSpec getSpec() {
        return m_spec;
    }

    @Override
    protected void save(final ModelContentWO model, final ExecutionMonitor exec) throws CanceledExecutionException {
        // nothing to do
    }

    @Override
    protected void load(final ModelContentRO model, final PortObjectSpec spec, final ExecutionMonitor exec)
            throws InvalidSettingsException, CanceledExecutionException {
        m_spec = (FabricWorkspacePortObjectSpec)spec;
    }

    @Override
    public JComponent[] getViews() {
        String text = FabricWorkspacePortViewFactory.createHtmlContent(getFabricConnection());
        JPanel f = ViewUtils.getInFlowLayout(new JLabel(text));
        f.setName("Connection");
        final JScrollPane scrollPane = new JScrollPane(f);
        scrollPane.setName("Connection");
        return new JComponent[]{scrollPane};
    }
}
