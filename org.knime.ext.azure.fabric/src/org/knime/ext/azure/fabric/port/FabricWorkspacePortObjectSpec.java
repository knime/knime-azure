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

import java.time.Duration;

import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;
import org.knime.core.node.port.AbstractSimplePortObjectSpec;
import org.knime.core.node.util.ViewUtils;
import org.knime.credentials.base.CredentialRef;

/**
 * Port object spec for the {@link FabricWorkspacePortObject}.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public final class FabricWorkspacePortObjectSpec extends AbstractSimplePortObjectSpec {

    private static final String CFG_SETTINGS = "settings";
    private static final String CFG_SETTINGS_WORKSPACE = "workspaceID";
    private static final String CFG_SETTINGS_CONNECTION_TIMEOUT = "connectionTimeout";
    private static final String CFG_SETTINGS_READ_TIMEOUT = "readTimeout";
    private static final String CFG_CREDENTIAL = "credentials";

    /** The serializer */
    public static final class Serializer
        extends AbstractSimplePortObjectSpecSerializer<FabricWorkspacePortObjectSpec> { }

    private FabricConnection m_connection;

    /**
     * Constructor for a port object spec that holds no {@link FabricConnection}.
     */
    public FabricWorkspacePortObjectSpec() {
        m_connection = null;
    }

    /**
     * Constructor for a port object spec that holds a {@link FabricConnection}.
     *
     * @param connection
     *            The {@link FabricConnection} that will be contained by this port
     *            object spec
     */
    public FabricWorkspacePortObjectSpec(final FabricConnection connection) {
        m_connection = connection;
    }

    /**
     * @return The contained {@link FabricConnection} object
     */
    public FabricConnection getFabricConnection() {
        return m_connection;
    }

    @Override
    protected void save(final ModelContentWO model) {
        final var settingsConfig = model.addConfig(CFG_SETTINGS);
        settingsConfig.addString(CFG_SETTINGS_WORKSPACE, m_connection.getWorkspaceId());
        settingsConfig.addLong(CFG_SETTINGS_CONNECTION_TIMEOUT, m_connection.getConnectionTimeout().toSeconds());
        settingsConfig.addLong(CFG_SETTINGS_READ_TIMEOUT, m_connection.getReadTimeout().toSeconds());

        final var credContent = model.addConfig(CFG_CREDENTIAL);
        m_connection.getCredential().save(credContent);
    }

    @Override
    protected void load(final ModelContentRO model) throws InvalidSettingsException {
        final var settingsConfig = model.getConfig(CFG_SETTINGS);
        final var workspaceId = settingsConfig.getString(CFG_SETTINGS_WORKSPACE);
        final var connectionTimeout = Duration.ofSeconds(settingsConfig.getLong(CFG_SETTINGS_CONNECTION_TIMEOUT));
        final var readTimeout = Duration.ofSeconds(settingsConfig.getLong(CFG_SETTINGS_READ_TIMEOUT));

        final var credConfig = model.getConfig(CFG_CREDENTIAL);
        final var credential = new CredentialRef();
        credential.load(credConfig);

        m_connection = new FabricConnection(credential, workspaceId, connectionTimeout, readTimeout);
    }

    @Override
    public boolean equals(final Object ospec) {
        if (this == ospec) {
            return true;
        }
        if (!(ospec instanceof FabricWorkspacePortObjectSpec)) {
            return false;
        }
        FabricWorkspacePortObjectSpec spec = (FabricWorkspacePortObjectSpec)ospec;
        return m_connection.equals(spec.m_connection);
    }

    @Override
    public int hashCode() {
        return m_connection != null ? m_connection.hashCode() : 0;
    }

    @Override
    public JComponent[] getViews() {
        String text;
        if (getFabricConnection() != null) {
            text = "<html>" + getFabricConnection().toString().replace("\n", "<br>") + "</html>";
        } else {
            text = "No connection available";
        }
        JPanel f = ViewUtils.getInFlowLayout(new JLabel(text));
        f.setName("Connection");
        return new JComponent[]{f};
    }
}
