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
 *   2025-05-03 (Bjoern Lohrmann): created
 */
package org.knime.ext.azure.onelake.filehandling.fs;

import java.time.Duration;

import org.knime.credentials.base.oauth.api.AccessTokenAccessor;
import org.knime.filehandling.core.connections.DefaultFSLocationSpec;
import org.knime.filehandling.core.connections.FSCategory;
import org.knime.filehandling.core.connections.FSLocationSpec;
import org.knime.filehandling.core.connections.meta.FSConnectionConfig;
import org.knime.filehandling.core.connections.meta.base.BaseFSConnectionConfig;

/**
 * {@link FSConnectionConfig} for OneLake.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class OneLakeFSConnectionConfig extends BaseFSConnectionConfig {

    /**
     * Default timeout in seconds for making connections and requests.
     */
    public static final int DEFAULT_TIMEOUT = 30;

    private final String m_workspaceName;

    private final String m_workspaceId;

    private AccessTokenAccessor m_tokenAccessor;

    private Duration m_connectionTimeout;

    private Duration m_readTimeout;

    /**
     * Constructor.
     *
     * @param workspaceName
     *            The Fabric workspace name
     * @param workspaceId
     *            The Fabric workspace ID (GUID)
     * @param workingDirectory
     *            The working directory to use for the file system connection.
     */
    public OneLakeFSConnectionConfig(final String workspaceName, //
            final String workspaceId, //
            final String workingDirectory) {
        super(workingDirectory, true);
        m_workspaceName = workspaceName;
        m_workspaceId = workspaceId;
        m_connectionTimeout = Duration.ofSeconds(DEFAULT_TIMEOUT);
        m_readTimeout = Duration.ofSeconds(DEFAULT_TIMEOUT);
    }

    /**
     * @return the Fabric workspace name
     */
    public String getWorkspaceName() {
        return m_workspaceName;
    }

    /**
     * @return the Fabric workspace ID (GUID)
     */
    public String getWorkspaceId() {
        return m_workspaceId;
    }

    /**
     * @return the {@link AccessTokenAccessor}
     */
    public AccessTokenAccessor getAccessTokenAccessor() {
        return m_tokenAccessor;
    }

    /**
     * @param tokenAccessor
     *            the {@link AccessTokenAccessor} to set
     */
    public void setAccessTokenAccessor(final AccessTokenAccessor tokenAccessor) {
        m_tokenAccessor = tokenAccessor;
    }

    /**
     * @return the connection timeout
     */
    public Duration getConnectionTimeout() {
        return m_connectionTimeout;
    }

    /**
     * @param timeout
     *            the connection timeout to set
     */
    public void setConnectionTimeout(final Duration timeout) {
        m_connectionTimeout = timeout;
    }

    /**
     * @return the read timeout
     */
    public Duration getReadTimeout() {
        return m_readTimeout;
    }

    /**
     * @param timeout
     *            the read timeout to set
     */
    public void setReadTimeout(final Duration timeout) {
        m_readTimeout = timeout;
    }

    /**
     * Creates a OneLake {@link FSLocationSpec} for the given workspace name.
     *
     * @param workspaceId
     *            the Fabric workspace ID (GUID)
     * @return a {@link FSLocationSpec} for the given workspace name
     */
    public static FSLocationSpec createFSLocationSpec(final String workspaceId) {
        return new DefaultFSLocationSpec(//
                FSCategory.CONNECTED, //
                String.format("%s:%s", OneLakeFSDescriptorProvider.FS_TYPE, workspaceId));

    }
}
