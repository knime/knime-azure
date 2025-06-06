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
 *   Nov 25, 2019 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.ext.azure.fabric.rest;

import java.time.Duration;
import java.util.Optional;

import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.core.Response.Status;

/**
 * Exception that describes a rate limit error, extending {@code ClientErrorException} from Jakarta.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class FabricRateLimitClientErrorException extends ClientErrorException {
    private static final long serialVersionUID = 1L;

    /**
     * Optional time from {@code retry-after} response header if present.
     */
    private final Duration m_retryAfter;

    /**
     * Constructor with default error message.
     *
     * @param retryAfter retry after response header, containing the seconds to wait or {@code null} if not present
     */
    public FabricRateLimitClientErrorException(final String retryAfter) {
        this(retryAfter, "Maximum number of requests per seconds has been exceeded, please try again later.");
    }

    /**
     * Constructor with error message from API response.
     *
     * @param retryAfter retry after response header, containing the seconds to wait or {@code null} if not present
     * @param message error message from API response
     */
    public FabricRateLimitClientErrorException(final String retryAfter, final String message) {
        super(message, Status.TOO_MANY_REQUESTS);
        m_retryAfter = parseRetryAfter(retryAfter).orElse(null);
    }

    /**
     * @return the optional time to wait from {@code retry-after} response header if present
     */
    public Optional<Duration> getRetryAfter() {
        return Optional.ofNullable(m_retryAfter);
    }

    private static Optional<Duration> parseRetryAfter(final String retryAfter) {
        try {
            final int seconds = Integer.parseInt(retryAfter);
            return Optional.of(Duration.ofSeconds(seconds));
        } catch (final NumberFormatException ex) { // NOSONAR
            return Optional.empty();
        }
    }

}
