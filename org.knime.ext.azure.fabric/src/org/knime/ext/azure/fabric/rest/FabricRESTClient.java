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
 *   Created on Aug 13, 2019 by Sascha Wolke, KNIME GmbH
 */
package org.knime.ext.azure.fabric.rest;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cxf.helpers.CastUtils;
import org.apache.cxf.interceptor.Fault;
import org.apache.cxf.jaxrs.client.ClientConfiguration;
import org.apache.cxf.jaxrs.client.JAXRSClientFactory;
import org.apache.cxf.jaxrs.client.ResponseExceptionMapper;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.message.Message;
import org.apache.cxf.phase.AbstractPhaseInterceptor;
import org.apache.cxf.phase.Phase;
import org.apache.cxf.transport.common.gzip.GZIPInInterceptor;
import org.apache.cxf.transport.http.asyncclient.AsyncHTTPConduit;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.credentials.base.NoSuchCredentialException;
import org.knime.credentials.base.oauth.api.AccessTokenAccessor;
import org.knime.ext.azure.fabric.node.connector.FabricCredentialUtil;
import org.knime.ext.azure.fabric.port.FabricConnection;
import org.knime.ext.azure.fabric.port.FabricWorkspacePortObjectSpec;
import org.knime.ext.azure.fabric.rest.sql.WarehouseAPI;
import org.knime.ext.azure.fabric.rest.workspace.WorkspaceAPI;
import org.knime.ext.azure.fabric.rest.wrapper.APIWrapper;
import org.knime.ext.azure.fabric.rest.wrapper.WarehouseAPIWrapper;
import org.knime.ext.azure.fabric.rest.wrapper.WorkspaceAPIWrapper;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.Version;

import com.fasterxml.jackson.jakarta.rs.json.JacksonJsonProvider;

import jakarta.ws.rs.core.MediaType;

/**
 * Client to access Microsoft Fabric REST API using CXF, JAX-RS and Jackson.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 * @author Sascha Wolke, KNIME GmbH
 */
public final class FabricRESTClient {

    /** Chunk threshold in bytes. */
    protected static final int CHUNK_THRESHOLD = 10 * 1024 * 1024; // 10MB

    /** Length in bytes of each chunk. */
    protected static final int CHUNK_LENGTH = 1 * 1024 * 1024; // 1MB

    private static final Version CLIENT_VERSION = FrameworkUtil.getBundle(FabricRESTClient.class).getVersion();
    private static final String USER_AGENT = "KNIME/" + CLIENT_VERSION;

    private static String BASE_URL = "https://api.fabric.microsoft.com/";


    private FabricRESTClient() {
    }

    /**
     * Creates a service proxy for given Microsoft Fabric REST API interface without
     * any authentication data. Use
     * {@link FabricRESTClient#create(String, Class, String, int, int)} or
     * {@link FabricRESTClient#create(String, Class, String, String, int, int)}
     * instead.
     *
     * @param deploymentUrl
     *            https://...cloud.databricks.com
     * @param proxy
     *            Interface to create proxy for
     * @param receiveTimeout
     *            Receive timeout
     * @param connectionTimeout
     *            connection timeout
     * @return Client implementation for given proxy interface
     */
    private static <T> T create(final Class<T> proxy, final Duration receiveTimeout,
        final Duration connectionTimeout, final ResponseExceptionMapper<?> exceptionMapper) {
        final HTTPClientPolicy clientPolicy = createClientPolicy(receiveTimeout, connectionTimeout);

        // Create the API Proxy
        final List<Object> provider = Arrays.asList(new JacksonJsonProvider(), exceptionMapper);
        final T proxyImpl = JAXRSClientFactory.create(BASE_URL, proxy, provider);
        WebClient.client(proxyImpl).accept(MediaType.APPLICATION_JSON_TYPE).type(MediaType.APPLICATION_JSON_TYPE)
                .header("User-Agent", USER_AGENT);
        final ClientConfiguration config = WebClient.getConfig(proxyImpl);
        config.getInInterceptors().add(new GZIPInInterceptor());
        // Note: Microsoft Fabric use GZIP to encode downloads, but does not support
        // GZIP encoded uploads!
        config.getHttpConduit().setClient(clientPolicy);

        // Enable request logging:
        // config.getInInterceptors().add(new LoggingInInterceptor());
        // config.getOutInterceptors().add(new LoggingOutInterceptor());

        // This forces usage of the Apache HTTP client over the JDK built-in HTTP client,
        // that does not work well with the strange configured Microsoft Fabric HTTP/2
        // endpoint, see BD-1242.
        config.getRequestContext().put(AsyncHTTPConduit.USE_ASYNC, Boolean.TRUE);

        return proxyImpl;
    }

    /**
     * @param receiveTimeoutMillis
     *            receive timeout
     * @param connectionTimeoutMillis
     *            connection timeout
     * @return default {@link HTTPClientPolicy} to use REST clients
     */
    protected static HTTPClientPolicy createClientPolicy(final Duration receiveTimeoutMillis,
            final Duration connectionTimeoutMillis) {
        final var clientPolicy = new HTTPClientPolicy();
        clientPolicy.setAllowChunking(true);
        clientPolicy.setChunkingThreshold(CHUNK_THRESHOLD);
        clientPolicy.setChunkLength(CHUNK_LENGTH);
        clientPolicy.setReceiveTimeout(receiveTimeoutMillis.toMillis());
        clientPolicy.setConnectionTimeout(connectionTimeoutMillis.toMillis());
        return clientPolicy;
    }


    private static <T> T create(final AccessTokenAccessor tokenAccessor, final Class<T> proxy,
            final Duration readTimeout, final Duration connectionTimeout) {

        final T proxyImpl = create(proxy, //
            readTimeout, //
            connectionTimeout, //
                new FabricResponseExceptionMapper());

        WebClient.getConfig(proxyImpl)//
            .getOutInterceptors()//
            .add(new FabricCredentialInterceptor(tokenAccessor));

        return wrap(proxyImpl);
    }

    /**
     * Creates a service proxy for given Microsoft Fabric REST API interface using a
     * single Workspace connection input port.
     *
     * Note that errors in this client are handled with
     * {@code ClientErrorException}.
     *
     * @param <T>
     *
     * @param proxy
     *            Interface to create proxy for
     * @param inSpecs
     *            the input specs with a single {@link CredentialPortObjectSpec}
     *            input port
     * @param readTimeout
     *            the read timeout
     * @param connectionTimeout
     *            the connection timeout
     * @return client implementation for given proxy interface
     * @throws InvalidSettingsException
     *             if the input port is invalid
     * @throws NoSuchCredentialException
     *             if the ingoing credential cannot be found anymore.
     * @throws IOException
     *             if there is an issue retrieving the actual Fabric-scoped access
     *             token
     */
    public static <T> T fromCredentialPort(final Class<T> proxy, final PortObjectSpec[] inSpecs,
            final Duration readTimeout, final Duration connectionTimeout)
            throws InvalidSettingsException, NoSuchCredentialException, IOException {

        if (inSpecs.length == 0) {
            throw new InvalidSettingsException("Missing input connection, Microsoft Authenticator required.");
        }

        if (inSpecs[0] instanceof CredentialPortObjectSpec credSpec) {
            final var tokenAccessor = FabricCredentialUtil.toAccessTokenAccessor(credSpec.toRef());
            return create(tokenAccessor, proxy, readTimeout, connectionTimeout);
        }

        throw new InvalidSettingsException("Invalid input connection, Microsoft Authenticator required.");
    }

    /**
     * Creates a service proxy for given Microsoft Fabric REST API interface using a
     * single Workspace connection input port.
     *
     * Note that errors in this client are handled with
     * {@code ClientErrorException}.
     *
     * @param <T>
     *
     * @param proxy
     *            Interface to create proxy for
     * @param inSpecs
     *            the input specs with a single
     *            {@link FabricWorkspacePortObjectSpec} input port
     * @return client implementation for given proxy interface
     * @throws InvalidSettingsException
     *             if the input port is invalid
     * @throws NoSuchCredentialException
     *             if the input credential is invalid
     * @throws IOException
     *             if there is an issue retrieving the actual Fabric-scoped access
     *             token
     */
    public static <T> T fromFabricPort(final Class<T> proxy, final PortObjectSpec[] inSpecs)
            throws InvalidSettingsException, NoSuchCredentialException, IOException {
        if (inSpecs.length == 0) {
            throw new InvalidSettingsException("Missing input connection, Microsoft Authenticator required.");
        }

        if (inSpecs[0] instanceof FabricWorkspacePortObjectSpec fabricWorkspaceSpec) {
            return fromFabricConnection(proxy, fabricWorkspaceSpec.getFabricConnection());
        }

        throw new InvalidSettingsException("Invalid input connection, Microsoft Fabric Workspace Connector required.");
    }

    /**
     * Creates a service proxy for given Microsoft Fabric REST API interface using a
     * single Workspace connection input port.
     *
     * Note that errors in this client are handled with
     * {@code ClientErrorException}.
     *
     * @param <T>
     *
     * @param proxy
     *            Interface to create proxy for
     * @param connection
     *            {@link FabricConnection} to use
     * @return client implementation for given proxy interface
     * @throws NoSuchCredentialException
     *             if the input credential is invalid
     * @throws IOException
     *             if there is an issue retrieving the actual Fabric-scoped access.
     *             token
     */
    public static <T> T fromFabricConnection(final Class<T> proxy, final FabricConnection connection)
            throws NoSuchCredentialException, IOException {

        final var tokenAccessor = FabricCredentialUtil.toAccessTokenAccessor(connection.getCredential());
        return create(tokenAccessor, proxy, connection.getReadTimeout(), connection.getConnectionTimeout());
    }

    @SuppressWarnings("unchecked")
    private static <T> T wrap(final T api) { // NOSONAR ignore to many returns
        if (api instanceof WorkspaceAPI) {
            return (T)new WorkspaceAPIWrapper((WorkspaceAPI)api);
        } else if (api instanceof WarehouseAPI) {
            return (T)new WarehouseAPIWrapper((WarehouseAPI)api);
        }

        throw new IllegalArgumentException("Unsupported API: " + api.getClass());
    }

    /**
     * Release the internal state and configuration associated with this service proxy.
     *
     * @param proxy Client proxy implementation
     */
    public static <T> void close(final T proxy) {
        Object toClose = proxy;
        if (proxy instanceof APIWrapper) {
            toClose = ((APIWrapper<?>)proxy).getWrappedAPI();
        }
        WebClient.client(toClose).close();
    }

    private static class FabricCredentialInterceptor extends AbstractPhaseInterceptor<Message> {

        final AccessTokenAccessor m_tokenAccessor;

        FabricCredentialInterceptor(final AccessTokenAccessor tokenAccessor) {
            super(Phase.SETUP);
            m_tokenAccessor = tokenAccessor;
        }

        @Override
        public void handleMessage(final Message message) throws Fault {
            @SuppressWarnings("unchecked")
            final Map<String, List<Object>> headers =
                CastUtils.cast((Map<String, List<Object>>)message.get(Message.PROTOCOL_HEADERS));

            try {
                final var authHeader = String.format("%s %s", //
                        m_tokenAccessor.getTokenType(), m_tokenAccessor.getAccessToken());
                headers.put("Authorization", List.of(authHeader));
            } catch (IOException ex) {
                throw new Fault(ex);
            }
        }
    }

}
