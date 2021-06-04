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
 *   2021-01-11 (Alexander Bondaletov): created
 */
package org.knime.ext.azure.adls.gen2.filehandling.fs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.knime.ext.azure.AzureUtils;

import com.azure.core.util.Context;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.FileRange;

/**
 * {@link InputStream} stream implementation to read files from ADLS. Reading is
 * performed by blocks.
 *
 * @author Alexander Bondaletov
 */
class AdlsInputStream extends InputStream {
    private static final int BLOCK_SIZE = 1024 * 1024;

    private final AdlsPath m_path;

    private long m_nextOffset;
    private byte[] m_buffer;
    private int m_bufferOffset;
    private boolean m_lastBlock;

    /**
     * @param path
     *            The file to read.
     * @throws IOException
     *
     */
    public AdlsInputStream(final AdlsPath path) throws IOException {
        m_path = path;
        m_buffer = new byte[0];
        m_bufferOffset = 0;
        m_nextOffset = 0;
        m_lastBlock = false;
        readNextBlockIfNecessary();
    }

    private void readNextBlockIfNecessary() throws IOException {
        if (!m_lastBlock && m_bufferOffset == m_buffer.length) {
            m_buffer = fetchNextBlock();
            m_bufferOffset = 0;
            m_nextOffset += m_buffer.length;
            m_lastBlock = m_buffer.length < BLOCK_SIZE;
        }
    }

    private byte[] fetchNextBlock() throws IOException {
        FileRange fileRange = new FileRange(m_nextOffset, (long) BLOCK_SIZE);

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            m_path.getFileClient().readWithResponse(out, fileRange, null, null, false, null, Context.NONE);
            return out.toByteArray();
        } catch (DataLakeStorageException ex) {
            if (ex.getStatusCode() == 416) {
                // Datalake API returns 416 error when the requested file is empty.
                return new byte[0];
            } else {
                throw AzureUtils.toIOE(ex, m_path.toString());
            }
        }
    }

    @Override
    public int read() throws IOException {
        readNextBlockIfNecessary();

        if (m_bufferOffset == m_buffer.length) {
            return -1;
        } else {
            final int indexToRead = m_bufferOffset;
            m_bufferOffset++;
            // return byte as int between 0 and 255
            return m_buffer[indexToRead] & 0xff;
        }
    }

    @Override
    public int read(final byte[] dest, final int off, final int len) throws IOException {
        readNextBlockIfNecessary();

        if (m_bufferOffset == m_buffer.length) {
            return -1;
        } else {
            final int bytesToRead = Math.min(len, m_buffer.length - m_bufferOffset);
            System.arraycopy(m_buffer, m_bufferOffset, dest, off, bytesToRead);
            m_bufferOffset += bytesToRead;
            return bytesToRead;
        }
    }
}
