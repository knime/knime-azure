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
package org.knime.ext.azure.fabric.port;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.credentials.base.CredentialRef;
import org.knime.credentials.base.NoSuchCredentialException;
import org.knime.ext.azure.fabric.rest.FabricRESTClient;

/**
 * Represents a connection to a Microsoft Fabric workspace. Use the
 * {@link #getAPI(Class)} method to obtain a proxy for the REST API interface.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
public class FabricConnection {

    private final CredentialRef m_credential;
    private final String m_workspaceId;
    private final Duration m_connectionTimeout;
    private final Duration m_readTimeout;

    /**
     * Constructor.
     *
     * @param credential
     *            {@link CredentialPortObjectSpec} with the credentials for Fabric
     * @param workspaceId
     *            the unique Fabric workspace id
     * @param connectionTimeout
     *            connection timeout
     * @param readTimeout
     *            read timeout
     *
     */
    public FabricConnection(final CredentialRef credential, final String workspaceId, final Duration connectionTimeout,
            final Duration readTimeout) {
        m_credential = credential;
        m_workspaceId = workspaceId;
        m_connectionTimeout = connectionTimeout;
        m_readTimeout = readTimeout;
    }

    /**
     * Returns the Fabric workspace id.
     *
     * @return the workspaceID
     */
    public String getWorkspaceId() {
        return m_workspaceId;
    }

    /**
     * @return the {@link CredentialRef}
     */
    public CredentialRef getCredential() {
        return m_credential;
    }

    /**
     * Creates a service proxy for given Microsoft Fabric REST API interface for
     * this Fabric workspace.
     *
     * Note that errors in this client are handled with
     * {@code ClientErrorException}.
     *
     * @param <T>
     *
     * @param proxy
     *            Interface to create proxy for
     * @return client implementation for given proxy interface
     * @throws NoSuchCredentialException
     *             if the input credential is invalid
     * @throws IOException
     *             if there is an issue retrieving the actual Fabric-scoped access.
     *             token
     */
    public <T> T getAPI(final Class<T> proxy) throws NoSuchCredentialException, IOException {
        return FabricRESTClient.fromFabricConnection(proxy, this);
    }

    /**
     * @return the readTimeout
     */
    public Duration getReadTimeout() {
        return m_readTimeout;
    }

    /**
     * @return the connectionTimeout
     */
    public Duration getConnectionTimeout() {
        return m_connectionTimeout;
    }

    @Override
    public int hashCode() {
        return Objects.hash(m_workspaceId, m_credential, m_connectionTimeout, m_readTimeout);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        FabricConnection other = (FabricConnection) obj;
        return Objects.equals(m_workspaceId, other.m_workspaceId) && Objects.equals(m_credential, other.m_credential)
                && m_connectionTimeout == other.m_connectionTimeout && m_readTimeout == other.m_readTimeout;
    }

    @Override
    public String toString() {
        return "Fabric Connection";
    }

}
