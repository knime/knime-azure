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
 *   2024-05-23 (Sascha Wolke, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.ext.azure.fabric.rest;


import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.cxf.jaxrs.client.ResponseExceptionMapper;

import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.ServerErrorException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * Map error HTTP status codes from response to Jakarta
 * {@link WebApplicationException}s with error message from response if
 * possible.
 *
 * The API response might have a JSON body that contains useful information.
 */
class FabricResponseExceptionMapper implements ResponseExceptionMapper<WebApplicationException> {

    @Override
    public WebApplicationException fromResponse(final Response response) { // NOSONAR it's simple enough...
        final MediaType mediaType = response.getMediaType();
        final String message = extractErrorMessage(response, mediaType);

        final WebApplicationException toReturn;
        if (response.getStatus() == 401 && !StringUtils.isBlank(message)) {
            toReturn = new NotAuthorizedException(message);
        } else if (response.getStatus() == 401) {
            toReturn = new NotAuthorizedException("Invalid or missing authentication data");
        } else if (response.getStatus() == 403 && !StringUtils.isBlank(message)) {
            toReturn = new ForbiddenException(message);
        } else if (response.getStatus() == 403) {
            toReturn = new ForbiddenException("Invalid or missing authentication data");
        } else if (response.getStatus() == 404 && !StringUtils.isBlank(message)) {
            toReturn = new NotFoundException(message);
        } else if (response.getStatus() == 404) {
            toReturn = new NotFoundException("Resource not found");
        } else if (response.getStatus() == 429 && !StringUtils.isBlank(message)) {
            toReturn = new FabricRateLimitClientErrorException(response.getHeaderString("retry-after"), message);
        } else if (response.getStatus() == 429) {
            toReturn = new FabricRateLimitClientErrorException(response.getHeaderString("retry-after"));
        } else if (response.getStatusInfo().getFamily() == Response.Status.Family.SERVER_ERROR
                && !StringUtils.isBlank(message)) {
            toReturn = new ServerErrorException("Server error: " + message, response.getStatus());
        } else if (response.getStatusInfo().getFamily() == Response.Status.Family.SERVER_ERROR) {
            toReturn = new ServerErrorException("Server error: " + response.getStatus(), response.getStatus());
        } else if (!StringUtils.isBlank(message)) {
            toReturn = new ClientErrorException("Server error: " + message, response.getStatus());
        } else {
            toReturn = new ClientErrorException("Server error: " + response.getStatus(), response.getStatus());
        }

        return toReturn;
    }

    private static String extractErrorMessage(final Response response, final MediaType mediaType) {
        String message = "";

        // try to parse JSON error response described here:
        // https://learn.microsoft.com/en-us/rest/api/fabric/core/workspaces/list-workspaces?tabs=HTTP#errorresponse
        if (mediaType != null && mediaType.getSubtype().toLowerCase(Locale.ENGLISH).contains("json")) {
            try {
                final ErrorResponse resp = response.readEntity(ErrorResponse.class);

                if (StringUtils.isNotBlank(resp.message) && StringUtils.isNotBlank(resp.errorCode)) {
                    message = "%s (error code: %s)".formatted(resp.message, resp.errorCode);
                } else if (StringUtils.isNotBlank(resp.message)) {
                    message = resp.message;
                } else if (StringUtils.isNotBlank(resp.errorCode)) {
                    message = resp.errorCode;
                } else {
                    message = response.getStatusInfo().getReasonPhrase();
                }
            } catch (final Exception e) { // NOSONAR
                message = e.getMessage();
            }
        } else if (mediaType != null && mediaType.getSubtype().toLowerCase(Locale.ENGLISH).contains("text")) {
            message = response.readEntity(String.class);
        }
        return message;
    }
}
