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
 *   2024-05-14 (Sascha Wolke, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.ext.azure.fabric.rest.workspace;

import java.io.IOException;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;

/**
 * REST API definition to access Fabric workspaces.
 *
 * @see <a href=
 *      "https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces">Workspacess
 *      API</a>
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 */
@Path("v1/workspaces")
public interface WorkspaceAPI {

    /**
     * Lists the first chunk of workspaces the current user has access to.
     *
     * @return list of workspaces
     * @throws IOException
     *             If an I/O error occured.
     * @throws WebApplicationException
     *             If the REST API returned an error.
     */
    @GET
    Workspaces listWorkspaces() throws IOException;

    /**
     * Lists the next chunk (based on continuation token) of workspaces the current
     * user has access to.
     *
     * @param continuationToken
     *            for pagination
     * @return list of workspaces
     * @throws IOException
     *             If an I/O error occured.
     * @throws WebApplicationException
     *             If the REST API returned an error.
     */
    @GET
    Workspaces listWorkspaces(@QueryParam("continuationToken") String continuationToken) throws IOException;

    /**
     * Retrieves a specific workspace by its ID.
     *
     * @param workspaceId
     *            The ID of the workspace to retrieve. This is the unique identifier
     * @return the {@link Workspace} with the given ID
     * @throws IOException
     *             If an I/O error occured.
     * @throws WebApplicationException
     *             If the REST API returned an error.
     */
    @GET
    @Path("{workspaceId}")
    Workspace getWorkspace(@PathParam("workspaceId") final String workspaceId) throws IOException;
}
