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
 *   Aug 11, 2016 (oole): created
 */
package org.knime.cloud.azure.abs.util;

import org.knime.base.filehandling.remote.files.Protocol;
import org.knime.cloud.azure.abs.filehandler.AzureBSConnection;
import org.knime.cloud.core.util.ConnectionInformationCloudSettings;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.ICredentials;

/**
 * Settings model representing the Azure Blob Store connection information
 *
 * @author Ole Ostergaard, KNIME.com GmbH
 */
public class AzureConnectionInformationSettings extends ConnectionInformationCloudSettings{

    private static final String SERVICE_NAME = "Azure Blob Store";

	public static final int DEFAULT_TIMEOUT = 30000;

	/**
	 * Constructor.
	 */
	public AzureConnectionInformationSettings(final String prefix) {
		super(prefix);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected SettingsModelAuthentication createAuthenticationModel()  {
		return new SettingsModelAuthentication("auth", AuthenticationType.USER_PWD, null, null, null);
	}

	/**
	 * @param credentialsProvider
	 * @param protocol
	 * @return
	 */
	@Override
	public CloudConnectionInformation createConnectionInformation(CredentialsProvider credentialsProvider,
			Protocol protocol) {

		// Create connection information object
		final CloudConnectionInformation connectionInformation = new CloudConnectionInformation();

		connectionInformation.setProtocol(protocol.getName());
		connectionInformation.setHost(AzureBSConnection.HOST);
		connectionInformation.setPort(protocol.getPort());
		connectionInformation.setTimeout(getTimeout());

		// Put storageAccount as user and accessKey as password
		if (useWorkflowCredential()) {
			// Use credentials
			final ICredentials credentials = credentialsProvider.get(getWorkflowCredential());
			connectionInformation.setUser(credentials.getLogin());
			connectionInformation.setPassword(credentials.getPassword());
		} else {
			connectionInformation.setUser(getUserValue());
			connectionInformation.setPassword(getPasswordValue());
		}

		connectionInformation.setServiceName(SERVICE_NAME);

		return connectionInformation;
	}
}