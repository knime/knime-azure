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
 *   2025-05-13 (Tobias): created
 */
package org.knime.ext.azure.fabric;

import java.io.IOException;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;
import org.knime.core.node.port.AbstractSimplePortObjectSpec.AbstractSimplePortObjectSpecSerializer;
import org.knime.credentials.base.CredentialPortObjectSpec;

/**
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class FabricConnection {

    private static final String CFG_SETTINGS = "settings";
    private static final String CFG_SETTINGS_WORKSPACE = "workspace";
    private static final String CFG_SETTINGS_CONNECTION_TIMEOUT = "connectionTimeout";
    private static final String CFG_SETTINGS_READ_TIMEOUT = "readTimeout";
    private static final String CFG_CREDENTIAL = "credentials";

    private final CredentialPortObjectSpec m_credentialSpec;
    private final String m_workspace;
    private final int m_connectionTimeout;
    private final int m_readTimeout;

    /**
     * @param credentialSpec
     *            {@link CredentialPortObjectSpec} with the credentials for Fabric
     * @param workspace
     * @param connectionTimeout
     * @param readTimeout
     *
     */
    public FabricConnection(final CredentialPortObjectSpec credentialSpec,
            final String workspace, final int connectionTimeout, final int readTimeout) {
        m_credentialSpec = credentialSpec;
        m_workspace = workspace;
        m_connectionTimeout = connectionTimeout;
        m_readTimeout = readTimeout;
    }

    /**
     * @param model
     * @throws InvalidSettingsException
     *
     */
    public FabricConnection(final ModelContentRO model) throws InvalidSettingsException {
        final var settingsConfig = model.getConfig(CFG_SETTINGS);
        m_workspace = settingsConfig.getString(CFG_SETTINGS_WORKSPACE);
        m_connectionTimeout = settingsConfig.getInt(CFG_SETTINGS_CONNECTION_TIMEOUT);
        m_readTimeout = settingsConfig.getInt(CFG_SETTINGS_READ_TIMEOUT);

        final var credConfig = model.getModelContent(CFG_CREDENTIAL);
        try {
            m_credentialSpec = AbstractSimplePortObjectSpecSerializer
                    .<CredentialPortObjectSpec>loadPortObjectSpecFromModelSettings(credConfig);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IOException ex) {
            throw new InvalidSettingsException(ex);
        }
    }

    /**
     * @param model
     */
    public void save(final ModelContentWO model) {
        final var settingsConfig = model.addConfig(CFG_SETTINGS);
        settingsConfig.addString(CFG_SETTINGS_WORKSPACE, m_workspace);
        settingsConfig.addInt(CFG_SETTINGS_CONNECTION_TIMEOUT, m_connectionTimeout);
        settingsConfig.addInt(CFG_SETTINGS_READ_TIMEOUT, m_readTimeout);

        final var credContent = model.addModelContent(CFG_CREDENTIAL);
        AbstractSimplePortObjectSpecSerializer
                .<CredentialPortObjectSpec>savePortObjectSpecToModelSettings(m_credentialSpec, credContent);
    }

    @Override
    public String toString() {
        return "FabricConnection [workspace=" + m_workspace + ", connectionTimeout=" + m_connectionTimeout
                + ", readTimeout=" + m_readTimeout + "]";
    }

}
