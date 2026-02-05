/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.rpc.protocol.tri.h12.grpc;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.utils.ArrayUtils;
import org.apache.dubbo.common.utils.ConcurrentHashMapUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.remoting.http12.exception.DecodeException;
import org.apache.dubbo.remoting.http12.exception.EncodeException;
import org.apache.dubbo.remoting.http12.exception.HttpStatusException;
import org.apache.dubbo.remoting.http12.message.HttpMessageCodec;
import org.apache.dubbo.remoting.http12.message.MediaType;
import org.apache.dubbo.rpc.model.FrameworkModel;
import org.apache.dubbo.rpc.model.MethodDescriptor;
import org.apache.dubbo.rpc.model.PackableMethod;
import org.apache.dubbo.rpc.model.PackableMethodFactory;
import org.apache.dubbo.rpc.protocol.tri.compressor.Compressor;
import org.apache.dubbo.rpc.protocol.tri.compressor.Identity;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.DUBBO_PACKABLE_METHOD_FACTORY;

/**
 * Codec for gRPC message frame format.
 *
 * <p>gRPC message frame format:
 * <pre>
 * +----------------------+
 * | Compressed-Flag (1B) |  0 = uncompressed, 1 = compressed
 * +----------------------+
 * | Message-Length  (4B) |  big-endian unsigned integer
 * +----------------------+
 * | Message Data    (N)  |  compressed or uncompressed payload
 * +----------------------+
 * </pre>
 */
public class GrpcCompositeCodec implements HttpMessageCodec {

    private static final String PACKABLE_METHOD_CACHE = "PACKABLE_METHOD_CACHE";

    private final URL url;

    private final FrameworkModel frameworkModel;

    private final String mediaType;

    private PackableMethod packableMethod;

    private Compressor compressor = Compressor.NONE;

    public GrpcCompositeCodec(URL url, FrameworkModel frameworkModel, String mediaType) {
        this.url = url;
        this.frameworkModel = frameworkModel;
        this.mediaType = mediaType;
    }

    public void setCompressor(Compressor compressor) {
        this.compressor = compressor;
    }

    public void loadPackableMethod(MethodDescriptor methodDescriptor) {
        if (methodDescriptor instanceof PackableMethod) {
            packableMethod = (PackableMethod) methodDescriptor;
            return;
        }

        packableMethod = ConcurrentHashMapUtils.computeIfAbsent(
                UrlUtils.computeServiceAttribute(
                        url, PACKABLE_METHOD_CACHE, k -> new ConcurrentHashMap<MethodDescriptor, PackableMethod>()),
                methodDescriptor,
                md -> frameworkModel
                        .getExtensionLoader(PackableMethodFactory.class)
                        .getExtension(ConfigurationUtils.getGlobalConfiguration(url.getApplicationModel())
                                .getString(DUBBO_PACKABLE_METHOD_FACTORY, DEFAULT_KEY))
                        .create(methodDescriptor, url, mediaType));
    }

    /**
     * Encode data with gRPC frame format and optional compression.
     *
     * <p>When outputStream is a ByteBufOutputStream, zero-copy encoding is used:
     * <ol>
     *   <li>Write compression flag (0 or 1)</li>
     *   <li>Write length placeholder (4 bytes)</li>
     *   <li>Serialize and optionally compress directly into ByteBuf</li>
     *   <li>Backfill the actual length</li>
     * </ol>
     *
     * <p>For other OutputStream types, buffered encoding is used as fallback.
     */
    @Override
    public void encode(OutputStream outputStream, Object data, Charset charset) throws EncodeException {
        try {
            if (outputStream instanceof ByteBufOutputStream) {
                encodeZeroCopy((ByteBufOutputStream) outputStream, data);
            } else {
                encodeBuffered(outputStream, data);
            }
        } catch (HttpStatusException e) {
            throw e;
        } catch (Exception e) {
            throw new EncodeException(e);
        }
    }

    /**
     * Zero-copy encoding: serialize and compress directly into ByteBuf.
     * On error, resets ByteBuf writerIndex to initial position to prevent memory leak.
     */
    private void encodeZeroCopy(ByteBufOutputStream bbos, Object data) throws Exception {
        boolean shouldCompress = !Identity.MESSAGE_ENCODING.equals(compressor.getMessageEncoding());
        ByteBuf buf = bbos.buffer();

        // Record initial position for rollback on error
        int initialWriterIndex = buf.writerIndex();
        OutputStream compressedStream = null;

        try {
            // Write compression flag (1 byte)
            buf.writeByte(shouldCompress ? 1 : 0);

            // Record position for length field, write placeholder (4 bytes)
            int lengthIndex = buf.writerIndex();
            buf.writeInt(0);

            // Serialize (and optionally compress) directly into ByteBuf
            OutputStream target = bbos;
            if (shouldCompress) {
                compressedStream = compressor.decorate(bbos);
                target = compressedStream;
            }
            packableMethod.packResponse(data, target);
            if (compressedStream != null) {
                compressedStream.close();
                compressedStream = null;
            }

            // Calculate and backfill actual length
            int messageLength = buf.writerIndex() - lengthIndex - 4;
            buf.setInt(lengthIndex, messageLength);
        } catch (Exception e) {
            // Rollback ByteBuf to initial position on error
            buf.writerIndex(initialWriterIndex);
            // Close compressed stream if still open
            if (compressedStream != null) {
                try {
                    compressedStream.close();
                } catch (IOException ignored) {
                    // Ignore close exception during error handling
                }
            }
            throw e;
        }
    }

    /**
     * Buffered encoding: fallback for non-ByteBuf streams.
     * Uses size() and writeTo() to avoid toByteArray() copy.
     */
    private void encodeBuffered(OutputStream outputStream, Object data) throws Exception {
        boolean shouldCompress = !Identity.MESSAGE_ENCODING.equals(compressor.getMessageEncoding());

        // Serialize message body
        ByteArrayOutputStream bodyBuffer = new ByteArrayOutputStream();
        packableMethod.packResponse(data, bodyBuffer);

        ByteArrayOutputStream frameBuffer;
        int compressedFlag;

        if (shouldCompress) {
            // Compress the serialized body
            frameBuffer = new ByteArrayOutputStream();
            OutputStream compressedOut = compressor.decorate(frameBuffer);
            try {
                bodyBuffer.writeTo(compressedOut);
            } finally {
                compressedOut.close();
            }
            compressedFlag = 1;
        } else {
            frameBuffer = bodyBuffer;
            compressedFlag = 0;
        }

        // Write gRPC frame header and data using size() and writeTo()
        outputStream.write(compressedFlag);
        writeLength(outputStream, frameBuffer.size());
        frameBuffer.writeTo(outputStream);
    }

    @Override
    public Object decode(InputStream inputStream, Class<?> targetType, Charset charset) throws DecodeException {
        try {
            return packableMethod.parseRequest(inputStream);
        } catch (HttpStatusException e) {
            throw e;
        } catch (Exception e) {
            throw new DecodeException(e);
        }
    }

    @Override
    public Object[] decode(InputStream inputStream, Class<?>[] targetTypes, Charset charset) throws DecodeException {
        Object message = decode(inputStream, ArrayUtils.isEmpty(targetTypes) ? null : targetTypes[0], charset);
        if (message instanceof Object[]) {
            return (Object[]) message;
        }
        return new Object[] {message};
    }

    private void writeLength(OutputStream outputStream, int length) {
        try {
            outputStream.write(((length >> 24) & 0xFF));
            outputStream.write(((length >> 16) & 0xFF));
            outputStream.write(((length >> 8) & 0xFF));
            outputStream.write((length & 0xFF));
        } catch (IOException e) {
            throw new EncodeException(e);
        }
    }

    @Override
    public MediaType mediaType() {
        return MediaType.APPLICATION_GRPC;
    }
}
