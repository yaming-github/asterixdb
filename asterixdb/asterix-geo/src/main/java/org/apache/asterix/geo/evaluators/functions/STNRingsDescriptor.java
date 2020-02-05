/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.geo.evaluators.functions;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptor;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCMultiPolygon;
import com.esri.core.geometry.ogc.OGCPolygon;

public class STNRingsDescriptor extends AbstractSTSingleGeometryDescriptor1 {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new STNRingsDescriptor();
        }
    };

//    @Override
//    protected Object evaluateOGCGeometry(OGCGeometry geometry) throws HyracksDataException {
//        if (geometry instanceof OGCPolygon) {
//            return ((OGCPolygon) geometry).numInteriorRing() + 1;
//        } else if (geometry instanceof OGCMultiPolygon) {
//            OGCMultiPolygon polygon = (OGCMultiPolygon) geometry;
//            int numGeometries = polygon.numGeometries();
//            int count = 0;
//            for (int i = 1; i < numGeometries + 1; i++) {
//                if (polygon.geometryN(i) instanceof OGCPolygon) {
//                    count += ((OGCPolygon) polygon.geometryN(i)).numInteriorRing() + 1;
//                }
//            }
//            return count;
//        } else {
//            throw new UnsupportedOperationException(
//                    "The operation " + getIdentifier() + " is not supported for the type " + geometry.geometryType());
//        }
//    }

    @Override
    protected Object evaluateOGCGeometry(byte[] bytes, int offset) throws HyracksDataException {
        int wkbType = bytes[offset] & 0xFF |
                (bytes[offset + 1] & 0xFF) << 8 |
                (bytes[offset + 2] & 0xFF) << 16 |
                (bytes[offset + 3] & 0xFF) << 24;
        offset += 4;
        switch(wkbType){
            case 3:
                return bytes[offset] & 0xFF |
                        (bytes[offset + 1] & 0xFF) << 8 |
                        (bytes[offset + 2] & 0xFF) << 16 |
                        (bytes[offset + 3] & 0xFF) << 24;
            case 6:
                int numPolygons = bytes[offset] & 0xFF |
                        (bytes[offset + 1] & 0xFF) << 8 |
                        (bytes[offset + 2] & 0xFF) << 16 |
                        (bytes[offset + 3] & 0xFF) << 24;
                offset += 4;
                int count = 0;
                int numRings, numPoints;
                for(int i = 0;i < numPolygons;i++){
                    offset += 5;
                    numRings = bytes[offset] & 0xFF |
                            (bytes[offset + 1] & 0xFF) << 8 |
                            (bytes[offset + 2] & 0xFF) << 16 |
                            (bytes[offset + 3] & 0xFF) << 24;
                    count += numRings;
                    offset += 4;
                    for(int j = 0;j < numRings;j++){
                        numPoints = bytes[offset] & 0xFF |
                                (bytes[offset + 1] & 0xFF) << 8 |
                                (bytes[offset + 2] & 0xFF) << 16 |
                                (bytes[offset + 3] & 0xFF) << 24;
                        offset += 4 + numPoints * 16;
                    }
                }
                return count;
            default:
                throw new UnsupportedOperationException("The operation is not supported for the type.");
        }
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_N_RINGS;
    }

}
