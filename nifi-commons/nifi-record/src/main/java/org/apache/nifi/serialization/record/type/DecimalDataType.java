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

package org.apache.nifi.serialization.record.type;

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;

public class DecimalDataType extends DataType {

    private final int precision;
    private final int scale;

    public DecimalDataType(int precision, int scale) {
        super(RecordFieldType.DECIMAL, null);
        this.precision = precision;
        this.scale = scale;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    @Override
    public RecordFieldType getFieldType() {
        return RecordFieldType.DECIMAL;
    }

    @Override
    public int hashCode() {
        return 31 + 41 * getFieldType().hashCode() + 41 * Integer.hashCode(precision) * Integer.hashCode(scale); // Need to confirm this hash * hash is correct
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof DecimalDataType)) {
            return false;
        }

        final DecimalDataType other = (DecimalDataType) obj;
        return precision == other.getPrecision() && scale == other.getScale();
    }

    @Override
    public String toString() {
        return "DECIMAL" + Integer.toString(precision) + Integer.toString(scale);
    }
}
