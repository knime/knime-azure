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
 *   2021-01-09 (Bjoern Lohrmann): created
 */
package org.knime.ext.azure.onelake.filehandling.testing;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import org.knime.credentials.base.oauth.api.AccessTokenCredential;
import org.knime.ext.azure.onelake.filehandling.fs.OneLakeFSConnection;
import org.knime.ext.azure.onelake.filehandling.fs.OneLakeFSConnectionConfig;
import org.knime.ext.azure.onelake.filehandling.fs.OneLakeFSDescriptorProvider;
import org.knime.ext.azure.onelake.filehandling.fs.OneLakeFileSystem;
import org.knime.ext.azure.onelake.filehandling.node.OneLakeCredentialUtil;
import org.knime.filehandling.core.connections.FSLocationSpec;
import org.knime.filehandling.core.connections.meta.FSType;
import org.knime.filehandling.core.testing.DefaultFSTestInitializerProvider;

import com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;

/**
 * Test initializer provider for OneLake.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class OneLakeFSTestInitializerProvider extends DefaultFSTestInitializerProvider {

    private static final String TENANT_ID = "tenantId";
    private static final String CLIENT_ID = "clientId";
    private static final String CLIENT_SECRET = "clientSecret";
    private static final String WORKSPACE_NAME = "workspaceName";
    private static final String WORKDIR_PREFIX = "workingDirPrefix";

    @SuppressWarnings("resource")
    @Override
    public OneLakeFSTestInitializer setup(final Map<String, String> configuration) throws IOException {

        final var workDir = generateRandomizedWorkingDir(getParameter(configuration, WORKDIR_PREFIX),
                OneLakeFileSystem.PATH_SEPARATOR);
        final var workspaceName = getParameter(configuration, WORKSPACE_NAME);

        // it is okay to pass workspace name instead of ID here
        final var config = new OneLakeFSConnectionConfig(workspaceName, workspaceName, workDir);

        config.setAccessTokenAccessor(fetchAccessToken(configuration));

        return new OneLakeFSTestInitializer(new OneLakeFSConnection(config));
    }

    private AccessTokenCredential fetchAccessToken(final Map<String, String> configuration) throws IOException {

        final String tokenEndpoint = "https://login.microsoftonline.com/%s/oauth2/v2.0/token"
                .formatted(getParameter(configuration, TENANT_ID));
        final String clientId = getParameter(configuration, CLIENT_ID);
        final String clientSecret = getParameter(configuration, CLIENT_SECRET);
        final String scope = OneLakeCredentialUtil.STORAGE_SCOPE;

        // Create the form body
        final RequestBody formBody = new FormBody.Builder()//
                .add("grant_type", "client_credentials")//
                .add("client_id", clientId)//
                .add("client_secret", clientSecret)//
                .add("scope", scope).build();

        final var client = new OkHttpClient();
        final var request = new Request.Builder()//
                .url(tokenEndpoint)//
                .post(formBody)
                .addHeader("Content-Type", "application/x-www-form-urlencoded")//
                .build();

        try (final var response = client.newCall(request).execute(); final var responseBody = response.body()) {
            if (response.isSuccessful()) {
                final var json = new ObjectMapper().readTree(responseBody.string());

                final var accessTokenString = json.get("access_token").asText();
                final var expiresAt = Instant.now().plusSeconds(json.get("expires_in").asInt());

                return new AccessTokenCredential(//
                        accessTokenString,//
                        expiresAt,//
                        "Bearer",//
                        null);

            } else {
                throw new IOException(
                        "Failed to fetch access token. HTTP code %d / Response: %s ".formatted(//
                                response.code(),
                                responseBody.string()
                        ));
            }
        }
    }

    @Override
    public FSType getFSType() {
        return OneLakeFSDescriptorProvider.FS_TYPE;
    }

    @Override
    public FSLocationSpec createFSLocationSpec(final Map<String, String> configuration) {
        return OneLakeFSConnectionConfig.createFSLocationSpec(//
                getParameter(configuration, WORKSPACE_NAME)); // it is okay to pass workspace name instead of ID here
    }
}
