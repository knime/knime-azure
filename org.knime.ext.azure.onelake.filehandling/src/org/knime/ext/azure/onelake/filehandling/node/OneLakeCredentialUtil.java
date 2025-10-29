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
 *   2025-05-26 (bjoern): created
 */
package org.knime.ext.azure.onelake.filehandling.node;

import java.io.IOException;
import java.util.Set;
import java.util.regex.Pattern;

import org.knime.core.node.InvalidSettingsException;
import org.knime.credentials.base.CredentialRef;
import org.knime.credentials.base.NoSuchCredentialException;
import org.knime.credentials.base.oauth.api.AccessTokenAccessor;
import org.knime.credentials.base.oauth.api.AccessTokenWithScopesAccessor;
import org.knime.credentials.base.oauth.api.IdentityProviderException;

/**
 * Utility class for handling credentials related to Microsoft OneLake.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public final class OneLakeCredentialUtil {

    /**
     * Azure Storage scope for OneLake access tokens.
     */
    public static final String STORAGE_SCOPE = "https://storage.azure.com/.default";

    private static final Pattern ERROR_PREFIX = Pattern.compile("^\\s*AADSTS(\\d+)", Pattern.CASE_INSENSITIVE); // NOSONAR

    private OneLakeCredentialUtil() {
        // Utility class, no instantiation
    }

    /**
     * Validates the provided {@link CredentialRef} to ensure it is compatible with
     * the Microsoft OneLake. Call this method only during the configure phase of
     * the node model.
     *
     * @param credRf
     *            the {@link CredentialRef} to validate
     * @throws InvalidSettingsException
     *             if the credential is not compatible
     */
    public static void validateCredentialOnConfigure(final CredentialRef credRf)
            throws InvalidSettingsException {

        if (credRf.isPresent()) {
            try {
                final var isCompatible = credRf.hasAccessor(AccessTokenAccessor.class)
                        || credRf.hasAccessor(AccessTokenWithScopesAccessor.class);

                if (!isCompatible) {
                    throw new InvalidSettingsException("Provided credential cannot be used with Microsoft OneLake");
                }
            } catch (NoSuchCredentialException ex) {
                throw new InvalidSettingsException(ex.getMessage(), ex);
            }
        }
    }

    /**
     * Resolves the given {@link CredentialRef} to an {@link AccessTokenAccessor}
     * with a Azure Storage-scoped access token that can be used to call Microsoft
     * OneLake APIs. This method should be called during the execute phase of a node
     * as it may perform I/O.
     *
     * @param credRef
     *            the {@link CredentialRef} to resolve.
     * @return an {@link AccessTokenAccessor} for Microsoft OneLake
     * @throws IOException
     *             if there is an issue retrieving the actual Storage-scoped access.
     *             token
     * @throws NoSuchCredentialException
     *             if the credential cannot be resolved or is incompatible
     */
    public static AccessTokenAccessor toAccessTokenAccessor(final CredentialRef credRef)
            throws IOException, NoSuchCredentialException {

        if (credRef.hasAccessor(AccessTokenAccessor.class)) {
            return credRef.toAccessor(AccessTokenAccessor.class);
        } else if (credRef.hasAccessor(AccessTokenWithScopesAccessor.class)) {
            try {
                return credRef.toAccessor(AccessTokenWithScopesAccessor.class)
                        .getAccessTokenWithScopes(Set.of(STORAGE_SCOPE));
            } catch (IdentityProviderException e) {
                throw handleIdentityProviderException(e);
            }
        } else {
            throw new IllegalStateException("The provided credential is incompatible with Microsoft OneLake");
        }
    }

    private static IOException handleIdentityProviderException(final IdentityProviderException e) {
        final var matcher = ERROR_PREFIX.matcher(e.getErrorSummary());
        if (matcher.find()) {
            final var errorCode = Integer.parseInt(matcher.group(1));
            // See
            // https://learn.microsoft.com/en-us/entra/identity-platform/reference-error-codes#aadsts-error-codes
            if (errorCode == 65001 || errorCode == 65004) {
                return new IOException("Consent mssing. Please refer to the node description to find "
                        + "scopes your or your admin need to consent to.", e);
            }
        }
        return e;
    }
}
