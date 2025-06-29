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
package org.knime.ext.azure.fabric.warehouse.node.connector;

import java.io.IOException;
import java.util.Set;

import org.knime.core.node.InvalidSettingsException;
import org.knime.credentials.base.CredentialPortObjectSpec;
import org.knime.credentials.base.CredentialRef;
import org.knime.credentials.base.NoSuchCredentialException;
import org.knime.credentials.base.oauth.api.AccessTokenAccessor;
import org.knime.credentials.base.oauth.api.AccessTokenWithScopesAccessor;

/**
 * Utility class for handling credentials related to Microsoft Fabric Data
 * Warehouse.
 *
 * @author Tobias Koetter, KNIME GmbH, Konstanz, Germany
 * @author Bjoern Lohrmann, KNIME GmbH
 */
final class FabricWarehouseCredentialUtil {

    private static final String DATABASE_SCOPE = "https://database.windows.net/.default";

    private FabricWarehouseCredentialUtil() {
        // Utility class, no instantiation
    }

    /**
     * Validates the provided {@link CredentialPortObjectSpec} to ensure it is
     * compatible with the Microsoft Fabric API. Call this method only during the
     * configure phase of the node model.
     *
     * @param credentialRef
     *            the {@link CredentialRef} to resolve.
     * @throws InvalidSettingsException
     *             if the credential is not compatible
     */
    public static void validateCredentialOnConfigure(final CredentialRef credentialRef)
            throws InvalidSettingsException {
        if (credentialRef.isPresent()) {
            try {
                final var isCompatible = credentialRef.hasAccessor(AccessTokenAccessor.class)
                        || credentialRef.hasAccessor(AccessTokenWithScopesAccessor.class);

                if (!isCompatible) {
                    throw new InvalidSettingsException(
                            "Provided credential cannot be used with Microsoft Fabric Data Warehouse");
                }
            } catch (NoSuchCredentialException ex) {
                throw new InvalidSettingsException(ex.getMessage(), ex);
            }
        }
    }

    /**
     * Resolves the given {@link CredentialRef} to an {@link AccessTokenAccessor}
     * with a Fabric-scoped access token that can be used to call Microsoft Fabric
     * APIs. This method should be called during the execute phase of a node as it
     * may perform I/O.
     *
     * @param credentialRef
     *            the {@link CredentialRef} to resolve.
     * @return an {@link AccessTokenAccessor} for Microsoft Fabric
     * @throws IOException
     *             if there is an issue retrieving the actual Fabric-scoped access.
     *             token
     * @throws NoSuchCredentialException
     *             if the credential cannot be resolved or is incompatible
     */
    public static AccessTokenAccessor toAccessTokenAccessor(final CredentialRef credentialRef)
            throws IOException, NoSuchCredentialException {

        if (credentialRef.hasAccessor(AccessTokenAccessor.class)) {
            return credentialRef.toAccessor(AccessTokenAccessor.class);
        } else if (credentialRef.hasAccessor(AccessTokenWithScopesAccessor.class)) {
            return credentialRef.toAccessor(AccessTokenWithScopesAccessor.class)
                    .getAccessTokenWithScopes(Set.of(DATABASE_SCOPE));
        } else {
            throw new IllegalStateException(
                    "The provided credential is incompatible with Microsoft Fabric Data Warehouse");
        }
    }
}
