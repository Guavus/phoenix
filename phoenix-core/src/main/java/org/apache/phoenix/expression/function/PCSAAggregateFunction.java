/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.expression.function;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.PCSACountClientAggregator;
import org.apache.phoenix.expression.aggregator.PCSACountServerAggregator;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.PCSAAggregateParseNode;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarbinary;


/**
 * 
 * Built-in function for SUM aggregation function.
 *
 * 
 * @since 0.1
 */
@BuiltInFunction(name=PCSAAggregateFunction.NAME, nodeClass=PCSAAggregateParseNode.class, args= {@Argument(allowedTypes={PVarbinary.class})} )
public class PCSAAggregateFunction extends DelegateConstantToCountAggregateFunction {
    public static final String NAME = "PCSA";
    
    public PCSAAggregateFunction() {
    }
    
    // TODO: remove when not required at built-in func register time
    public PCSAAggregateFunction(List<Expression> childExpressions){
        super(childExpressions, null);
    }
    
    public PCSAAggregateFunction(List<Expression> childExpressions, CountAggregateFunction delegate){
        super(childExpressions, delegate);
    }
    
    @Override
    public Aggregator newClientAggregator() {
        return new PCSACountClientAggregator(SortOrder.getDefault());
    }
    
    @Override
    public Aggregator newServerAggregator(Configuration conf) {
        Expression child = getAggregatorExpression();
        return new PCSACountServerAggregator(child.getSortOrder());
    }
    
    @Override
    public Aggregator newServerAggregator(Configuration conf, ImmutableBytesWritable ptr) {
    	Expression child = getAggregatorExpression();
        return new PCSACountServerAggregator(child.getSortOrder());
    }
    
    @Override
    public PDataType getDataType() {
          return PLong.INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
