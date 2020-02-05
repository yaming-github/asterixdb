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

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryCursor;
import com.esri.core.geometry.MultiVertexGeometry;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.ogc.OGCGeometry;

public class STNPointsDescriptor extends AbstractSTSingleGeometryDescriptor1 {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override
        public IFunctionDescriptor createFunctionDescriptor() {
            return new STNPointsDescriptor();
        }
    };

//    @Override
//    protected Object evaluateOGCGeometry(OGCGeometry geometry) throws HyracksDataException {
//        Geometry esriGeometry = geometry.getEsriGeometry();
//        if (esriGeometry instanceof MultiVertexGeometry) {
//            return ((MultiVertexGeometry) esriGeometry).getPointCount();
//        } else if (esriGeometry instanceof Point) {
//            return 1;
//        } else if (esriGeometry == null) {
//            int count = 0;
//            GeometryCursor geometryCursor = geometry.getEsriGeometryCursor();
//            esriGeometry = geometryCursor.next();
//            while (esriGeometry != null) {
//                if (esriGeometry instanceof MultiVertexGeometry) {
//                    count += ((MultiVertexGeometry) esriGeometry).getPointCount();
//                } else if (esriGeometry instanceof Point) {
//                    count += 1;
//                }
//                esriGeometry = geometryCursor.next();
//            }
//            return count;
//        } else if (geometry.isEmpty()) {
//            return 0;
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
            case 1:
                return 1;
            case 2:
            case 4:
                return bytes[offset] & 0xFF |
                        (bytes[offset + 1] & 0xFF) << 8 |
                        (bytes[offset + 2] & 0xFF) << 16 |
                        (bytes[offset + 3] & 0xFF) << 24;
            case 3:
                int numRings = bytes[offset] & 0xFF |
                        (bytes[offset + 1] & 0xFF) << 8 |
                        (bytes[offset + 2] & 0xFF) << 16 |
                        (bytes[offset + 3] & 0xFF) << 24;
                int count = 0;
                offset += 4;
                int num;
                for(int i = 0;i < numRings;i++){
                    num = bytes[offset] & 0xFF |
                            (bytes[offset + 1] & 0xFF) << 8 |
                            (bytes[offset + 2] & 0xFF) << 16 |
                            (bytes[offset + 3] & 0xFF) << 24;
                    count += num - 1;
                    offset += 4 + num * 16;
                }
                return count;
            case 5:
                int numLineStrings = bytes[offset] & 0xFF |
                        (bytes[offset + 1] & 0xFF) << 8 |
                        (bytes[offset + 2] & 0xFF) << 16 |
                        (bytes[offset + 3] & 0xFF) << 24;
                offset += 4;
                count = 0;
                for(int i = 0;i < numLineStrings;i++){
                    offset += 5;
                    num = bytes[offset] & 0xFF |
                            (bytes[offset + 1] & 0xFF) << 8 |
                            (bytes[offset + 2] & 0xFF) << 16 |
                            (bytes[offset + 3] & 0xFF) << 24;
                    count += num;
                    offset += 4 + num * 16;
                }
                return count;
            case 6:
                int numPolygons = bytes[offset] & 0xFF |
                        (bytes[offset + 1] & 0xFF) << 8 |
                        (bytes[offset + 2] & 0xFF) << 16 |
                        (bytes[offset + 3] & 0xFF) << 24;
                offset += 4;
                count = 0;
                for(int i = 0;i < numPolygons;i++){
                    offset += 5;
                    numRings = bytes[offset] & 0xFF |
                            (bytes[offset + 1] & 0xFF) << 8 |
                            (bytes[offset + 2] & 0xFF) << 16 |
                            (bytes[offset + 3] & 0xFF) << 24;
                    offset += 4;
                    for(int j = 0;j < numRings;j++){
                        num = bytes[offset] & 0xFF |
                                (bytes[offset + 1] & 0xFF) << 8 |
                                (bytes[offset + 2] & 0xFF) << 16 |
                                (bytes[offset + 3] & 0xFF) << 24;
                        count += num - 1;
                        offset += 4 + num * 16;
                    }
                }
                return count;
            default:
                throw new UnsupportedOperationException("The operation is not supported for the type ");
        }
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.ST_N_POINTS;
    }

}
