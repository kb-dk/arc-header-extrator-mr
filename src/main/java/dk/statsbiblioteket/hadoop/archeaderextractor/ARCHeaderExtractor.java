package dk.statsbiblioteket.hadoop.archeaderextractor;

/*
 * #%L
 * ARC Header Extractor MR
 * %%
 * Copyright (C) 2013 State and University Library, Denmark
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.apache.commons.codec.digest.DigestUtils;
import org.jwat.arc.ArcReaderFactory;
import org.jwat.arc.ArcRecordBase;
import org.jwat.common.*;
import org.jwat.arc.ArcReader;


import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * User: perdalum
 * Date: 08/08/13
 * Time: 11.34
 */

public class ARCHeaderExtractor {

    private static boolean hasPayload(ArcRecordBase arcRecord) throws IOException {
        Payload payload = arcRecord.getPayload();
        boolean result = payload != null;
        if (payload != null) {
            if (payload.getTotalLength() == 0) {
                result = false;
            }
            payload.close();
        }
        return result;
    }

    private static String extractHeader(ArcRecordBase arcRecord, String timeStamp) throws IOException {
        StringWriter headerString = new StringWriter();

        HttpHeader httpHeader = null;
        if (hasPayload(arcRecord)) {
            httpHeader = arcRecord.getHttpHeader();
            if (httpHeader != null) {

                headerString.write("Offset: " + arcRecord.getStartOffset() + "\n");

                String URLHash = DigestUtils.md5Hex(arcRecord.getUrlStr());

                headerString.write("KEY: " + URLHash + "-" + timeStamp + "\n");
                headerString.write("URL: " + arcRecord.getUrlStr() + "\n");

                headerString.write("IP:  " + arcRecord.getIpAddress() + "\n");

                headerString.write("ProtocolVersion: " + httpHeader.getProtocolVersion() + "\n");
                headerString.write("ProtocolStatusCode: " + httpHeader.getProtocolStatusCodeStr() + "\n");
                headerString.write("ProtocolContentType: " + httpHeader.getProtocolContentType() + "\n");
                headerString.write("TotalLength: " + httpHeader.getTotalLength() + "\n");

                for (HeaderLine hl : httpHeader.getHeaderList()) {
                    headerString.write(hl.name + ": " + hl.value + "\n");
                }
            }
        }
        if (httpHeader != null) {
            httpHeader.close();
        }
        arcRecord.close();
        return headerString.toString();
    }

    private static Map<Long, String> parse(File arcFile) throws IOException {
        Map<Long, String> headers = new HashMap<Long, String>();

        /*
        String lastModifiedString = String.valueOf(arcFile.lastModified());
        headers.put((long) 1, lastModifiedString);
        return headers;
        */

        /*  extract the time stamp from the file name of the ARC file
            The time stamp is the third element of the file name when split on "-".
         */

        String timeStamp = arcFile.getName().split("-")[2];

        RandomAccessFile raf;
        RandomAccessFileInputStream rafin;
        ByteCountingPushBackInputStream pbin;

        ArcReader arcReader = null;
        ArcRecordBase arcRecord;
        UriProfile uriProfile = UriProfile.RFC3986;

        boolean bBlockDigestEnabled = true;
        boolean bPayloadDigestEnabled = true;
        int recordHeaderMaxSize = 8192;
        int payloadHeaderMaxSize = 32768;

        raf = new RandomAccessFile(arcFile, "r");
        rafin = new RandomAccessFileInputStream(raf);
        pbin = new ByteCountingPushBackInputStream(new BufferedInputStream(rafin, 8192), 16);

        if (ArcReaderFactory.isArcFile(pbin)) {
            arcReader = ArcReaderFactory.getReaderUncompressed(pbin);
            arcReader.setUriProfile(uriProfile);
            arcReader.setBlockDigestEnabled(bBlockDigestEnabled);
            arcReader.setPayloadDigestEnabled(bPayloadDigestEnabled);
            arcReader.setRecordHeaderMaxSize(recordHeaderMaxSize);
            arcReader.setPayloadHeaderMaxSize(payloadHeaderMaxSize);

            while ((arcRecord = arcReader.getNextRecord()) != null) {
                headers.put(arcRecord.getStartOffset(), extractHeader(arcRecord, timeStamp));
            }
            arcReader.close();
        } else {
            System.err.println("Input file is not an ARC file");
        }

        if (arcReader != null) {
            arcReader.close();
        }
        pbin.close();
        raf.close();

        return headers;

    }

    public Map<Long, String> extract(String filePath) throws IOException {
        File file = new File(filePath);
        Map<Long, String> allHeaders;

        if (!file.isFile()) {
            System.err.println("Input parameter '" + filePath + "' is not a file!");
        }

        allHeaders = parse(file);
        return allHeaders;
    }
}
