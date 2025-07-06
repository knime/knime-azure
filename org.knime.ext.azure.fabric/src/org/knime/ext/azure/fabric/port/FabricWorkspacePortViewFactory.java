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
 *   2023-04-11 (Alexander Bondaletov, Redfield SE): created
 */
package org.knime.ext.azure.fabric.port;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.knime.core.webui.data.InitialDataService;
import org.knime.core.webui.data.RpcDataService;
import org.knime.core.webui.node.port.PortSpecViewFactory;
import org.knime.core.webui.node.port.PortView;
import org.knime.core.webui.node.port.PortViewFactory;
import org.knime.core.webui.node.port.PortViewManager;
import org.knime.core.webui.node.port.PortViewManager.PortViewDescriptor;
import org.knime.core.webui.page.Page;
import org.knime.credentials.base.Credential;
import org.knime.credentials.base.CredentialPortViewData;
import org.knime.credentials.base.CredentialPortViewData.Section;

/**
 * {@link PortViewFactory} for the {@link FabricWorkspacePortObject}.
 *
 * @author Jannik Löscher, KNIME GmbH, Konstanz, Germany
 */
@SuppressWarnings("restriction")
public final class FabricWorkspacePortViewFactory {

    private FabricWorkspacePortViewFactory() {
    }

    /**
     * Registers the views with the {@link PortViewManager}.
     */
    public static void register() {
        final var portSpecViewFactory = //
                (PortSpecViewFactory<FabricWorkspacePortObjectSpec>) FabricWorkspacePortViewFactory::createPortSpecView;
        final var portViewFactory = //
                (PortViewFactory<FabricWorkspacePortObject>) FabricWorkspacePortViewFactory::createPortView;
        PortViewManager.registerPortViews(FabricWorkspacePortObject.class, //
                List.of(new PortViewDescriptor("Fabric Workspace Connection", portSpecViewFactory), //
                        new PortViewDescriptor("Fabric Workspace Connection", portViewFactory)), //
                List.of(0), //
                List.of(1));
    }

    static String createHtmlContent(final FabricConnection connection) {
        final var content = Optional.ofNullable(connection) //
                .map(FabricWorkspacePortViewFactory::renderPortViewData) //
                .orElse("No connection available.");
        return createHtmlPage(content);
    }

    private static String createHtmlPage(final String content) {
        final var sb = new StringBuilder();
        sb.append("<html><head><style>\n");
        try (var in = FabricWorkspacePortViewFactory.class.getClassLoader().getResourceAsStream("table.css")) {
            sb.append(new String(in.readAllBytes(), StandardCharsets.UTF_8)); //
        } catch (IOException ex) { // NOSONAR ignore, should always work
        }
        sb.append("</style></head><body>\n");
        sb.append(content);
        sb.append("</body></html>\n");
        return sb.toString();
    }

    private static Section getConnectionData(final FabricConnection connection) {
        return new Section("Connection Settings", new String[][] { //
                new String[] { "Property", "Value" }, //
                new String[] { "Workspace ID", connection.getWorkspaceId() }, //
                new String[] { "Connection timeout", connection.getConnectionTimeout().toSeconds() + " seconds" }, //
                new String[] { "Read timeout", connection.getReadTimeout().toSeconds() + " seconds" } //
        });
    }

    private static String renderPortViewData(final FabricConnection connection) {
        final var sb = new StringBuilder();
        final var data = new LinkedList<Section>();

        data.add(getConnectionData(connection));
        connection.getCredential().getCredential(Credential.class).map(Credential::describe)
                .map(CredentialPortViewData::sections).ifPresent(data::addAll);

        for (var section : data) {
            sb.append(String.format("<h4>%s</h4>%n", section.title()));

            sb.append("<table>\n");

            final var columns = section.columns();

            // render first row as table header
            if (columns.length >= 1) {
                sb.append("<tr>\n");
                sb.append(Arrays.stream(columns[0])//
                        .map(h -> String.format("<th>%s</th>%n", h))//
                        .collect(Collectors.joining()));
                sb.append("</tr>\n");
            }

            for (var i = 1; i < columns.length; i++) {
                sb.append("<tr>\n");
                sb.append(Arrays.stream(columns[i])//
                        .map(h -> String.format("<td>%s</td>%n", h))//
                        .collect(Collectors.joining()));
                sb.append("</tr>\n");
            }
            sb.append("</table>\n");
        }

        return sb.toString();
    }

    private static PortView createPortView(final FabricWorkspacePortObject portObject) {
        return new PortView() {
            @Override
            public Page getPage() {
                return Page.builder(() -> createHtmlContent(portObject.getFabricConnection()), "index.html").build();
            }

            @SuppressWarnings("unchecked")
            @Override
            public Optional<InitialDataService<?>> createInitialDataService() {
                return Optional.empty();
            }

            @Override
            public Optional<RpcDataService> createRpcDataService() {
                return Optional.empty();
            }
        };
    }

    /**
     * @param pos
     *            The port object spec.
     */
    private static PortView createPortSpecView(final FabricWorkspacePortObjectSpec pos) {
        return new PortView() {
            @Override
            public Page getPage() {
                return Page.builder(() -> createHtmlContent(pos.getFabricConnection()), "index.html").build();
            }

            @SuppressWarnings("unchecked")
            @Override
            public Optional<InitialDataService<?>> createInitialDataService() {
                return Optional.empty();
            }

            @Override
            public Optional<RpcDataService> createRpcDataService() {
                return Optional.empty();
            }
        };
    }
}
