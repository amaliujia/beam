package org.apache.beam.sdk.extensions.sql.codegen;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamJavaTypeFactory;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator.InputGetter;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.validate.SqlConformance;

public class CodeGenerator implements RexVisitor<Expression> {
  private final JavaTypeFactory typeFactory;
  private final RexBuilder builder;
  private final RexProgram program;
  private final SqlConformance conformance;
  private final org.apache.calcite.adapter.enumerable.RexToLixTranslator.InputGetter inputGetter;
  private final BlockBuilder list;
  private final Map<? extends RexNode, Boolean> exprNullableMap;
  private final Function1<String, InputGetter> correlates;


  public CodeGenerator() {
    this.program = null; // may be null
    this.typeFactory = BeamJavaTypeFactory.INSTANCE;
    this.conformance = null;
    this.inputGetter = null;
    this.list = null;
    this.exprNullableMap = null;
    this.builder = null;
    this.correlates = null; // may be null
  }

  public CodeGenerator(RexProgram program,
      JavaTypeFactory typeFactory,
      InputGetter inputGetter,
      BlockBuilder list,
      Map<? extends RexNode, Boolean> exprNullableMap,
      RexBuilder builder,
      SqlConformance conformance,
      Function1<String, InputGetter> correlates) {
    this.program = program; // may be null
    this.typeFactory = Objects.requireNonNull(typeFactory);
    this.conformance = Objects.requireNonNull(conformance);
    this.inputGetter = inputGetter;
    this.list = Objects.requireNonNull(list);
    this.exprNullableMap = Objects.requireNonNull(exprNullableMap);
    this.builder = Objects.requireNonNull(builder);
    this.correlates = correlates; // may be null
  }

  public List<Expression> translateProjects() {

    // List<Type> storageTypes = null;
    // if (outputPhysType != null) {
    //   final RelDataType rowType = outputPhysType.getRowType();
    //   storageTypes = new ArrayList<>(rowType.getFieldCount());
    //   for (int i = 0; i < rowType.getFieldCount(); i++) {
    //     storageTypes.add(outputPhysType.getJavaFieldType(i));
    //   }
    // }

    // RexToLixTranslator rexToLixTranslator = new RexToLixTranslator(program, typeFactory, root, inputGetter,
    //     list, Collections.emptyMap(), new RexBuilder(typeFactory), conformance,
    //     null, null);

    // return rexToLixTranslator.translateList(program.getProjectList(), storageTypes);
    return program.getProjectList().stream().map(this::translateLocalRef).collect(Collectors.toList());
  }

  public Expression translateLocalRef(RexNode rexNode) {
    return rexNode.accept(this);
  }

  @Override
  public Expression visitInputRef(RexInputRef inputRef) {
    int index = inputRef.getIndex();
    // Expression x = inputGetter.field(list, index, storageType);
    // Expression input = list.append("inp" + index + "_", x); // safe to share
    return null;
  }

  @Override
  public Expression visitLocalRef(RexLocalRef localRef) {
    return null;
  }

  @Override
  public Expression visitLiteral(RexLiteral literal) {
    Type returnType = typeFactory.getJavaClass(literal.getType());
    if (literal.isNull()) {
      return RexImpTable.NULL_EXPR;
    }

    final Object returnValue;
    switch (literal.getType().getSqlTypeName()) {
      case BOOLEAN:
      case INTEGER:
      case TINYINT:
      case SMALLINT:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
        // primitive
        Primitive primitive = Primitive.ofBoxOr(returnType);
        Comparable value = literal.getValueAs(Comparable.class);
        if (primitive != null && value instanceof Number) {
          returnValue = primitive.number((Number) value);
        } else {
          returnValue = value;
        }
        break;
      case DECIMAL:
        final BigDecimal bd = literal.getValueAs(BigDecimal.class);
        if (returnType == float.class) {
          return Expressions.constant(bd, returnType);
        } else if (returnType == double.class) {
          return Expressions.constant(bd, returnType);
        }
        assert returnType == BigDecimal.class;
        return Expressions.new_(BigDecimal.class,
            Expressions.constant(bd.toString()));
      case DATE:
      case TIME_WITH_LOCAL_TIME_ZONE:
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
        returnValue = literal.getValueAs(Integer.class);
        returnType = int.class;
        break;
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        returnValue = literal.getValueAs(Long.class);
        returnType = long.class;
        break;
      case CHAR:
      case VARCHAR:
        returnValue = literal.getValueAs(String.class);
        break;
      case BINARY:
      case VARBINARY:
        return Expressions.new_(
            ByteString.class,
            Expressions.constant(
                literal.getValueAs(byte[].class),
                byte[].class));
      case TIME:
        // TODO: Can integer hold nanosecond precision?
        returnValue = literal.getValueAs(Integer.class);
        returnType = int.class;
        break;
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        // TODO: TIMESTAMP should use java.sql.timestamp (which has an extra int for nanosecond)
        returnValue = literal.getValueAs(Long.class);
        returnType = long.class;
        break;      default:
        throw new RuntimeException("Cannot translate RexLiteral with sql type: " + literal.getType().getSqlTypeName());
    }

    return Expressions.constant(returnValue, returnType);
  }

  @Override
  public Expression visitCall(RexCall call) {
    Type returnType = typeFactory.getJavaClass(call.getType());

    return null;
  }

  @Override
  public Expression visitOver(RexOver over) {
    return null;
  }

  @Override
  public Expression visitCorrelVariable(RexCorrelVariable correlVariable) {
    return null;
  }

  @Override
  public Expression visitDynamicParam(RexDynamicParam dynamicParam) {
    return null;
  }

  @Override
  public Expression visitRangeRef(RexRangeRef rangeRef) {
    return null;
  }

  @Override
  public Expression visitFieldAccess(RexFieldAccess fieldAccess) {
    return null;
  }

  @Override
  public Expression visitSubQuery(RexSubQuery subQuery) {
    return null;
  }

  @Override
  public Expression visitTableInputRef(RexTableInputRef fieldRef) {
    return null;
  }

  @Override
  public Expression visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    return null;
  }
}
