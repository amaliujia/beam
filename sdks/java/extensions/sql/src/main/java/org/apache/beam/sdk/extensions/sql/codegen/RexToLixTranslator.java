package org.apache.beam.sdk.extensions.sql.codegen;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator.AlwaysNull;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator.InputGetter;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.util.ControlFlowException;

public class RexToLixTranslator {
  final JavaTypeFactory typeFactory;
  final RexBuilder builder;
  private final RexProgram program;
  final SqlConformance conformance;
  private final Expression root;
  private final org.apache.calcite.adapter.enumerable.RexToLixTranslator.InputGetter inputGetter;
  private final BlockBuilder list;
  private final Map<? extends RexNode, Boolean> exprNullableMap;
  private final org.apache.calcite.adapter.enumerable.RexToLixTranslator parent;
  private final Function1<String, InputGetter> correlates;

  private RexToLixTranslator(RexProgram program,
      JavaTypeFactory typeFactory,
      Expression root,
      InputGetter inputGetter,
      BlockBuilder list,
      Map<? extends RexNode, Boolean> exprNullableMap,
      RexBuilder builder,
      SqlConformance conformance,
      org.apache.calcite.adapter.enumerable.RexToLixTranslator parent,
      Function1<String, InputGetter> correlates) {
    this.program = program; // may be null
    this.typeFactory = Objects.requireNonNull(typeFactory);
    this.conformance = Objects.requireNonNull(conformance);
    this.root = Objects.requireNonNull(root);
    this.inputGetter = inputGetter;
    this.list = Objects.requireNonNull(list);
    this.exprNullableMap = Objects.requireNonNull(exprNullableMap);
    this.builder = Objects.requireNonNull(builder);
    this.parent = parent; // may be null
    this.correlates = correlates; // may be null
  }

  public static List<Expression> translateProjects(RexProgram program,
      JavaTypeFactory typeFactory, SqlConformance conformance,
      BlockBuilder list, PhysType outputPhysType, Expression root,
      InputGetter inputGetter, Function1<String, InputGetter> correlates) {

    List<Type> storageTypes = null;
    if (outputPhysType != null) {
      final RelDataType rowType = outputPhysType.getRowType();
      storageTypes = new ArrayList<>(rowType.getFieldCount());
      for (int i = 0; i < rowType.getFieldCount(); i++) {
        storageTypes.add(outputPhysType.getJavaFieldType(i));
      }
    }

    RexToLixTranslator rexToLixTranslator = new RexToLixTranslator(program, typeFactory, root, inputGetter,
        list, Collections.emptyMap(), new RexBuilder(typeFactory), conformance,
        null, null);

    return rexToLixTranslator.translateList(program.getProjectList(), storageTypes);
  }

  Expression translate(RexNode expr) {
    final RexImpTable.NullAs nullAs =
        RexImpTable.NullAs.of(isNullable(expr));
    return translate(expr, nullAs);
  }

  Expression translate(RexNode expr, RexImpTable.NullAs nullAs) {
    return translate(expr, nullAs, null);
  }

  Expression translate(RexNode expr, Type storageType) {
    final RexImpTable.NullAs nullAs =
        RexImpTable.NullAs.of(isNullable(expr));
    return translate(expr, nullAs, storageType);
  }

  Expression translate(RexNode expr, RexImpTable.NullAs nullAs,
      Type storageType) {
    Expression expression = translate0(expr, nullAs, storageType);
    expression = EnumUtils.enforce(storageType, expression);
    assert expression != null;
    return list.append("v", expression);
  }

  // Translate the list of {@code RexNoe}.
  // TODO: optimize for output storage.
  private List<Expression> translateList(List<? extends RexNode> operandList,
      List<? extends Type> storageTypes) {
    final List<Expression> list = new ArrayList<>(operandList.size());
    for (int i = 0; i < operandList.size(); i++) {
      RexNode rex = operandList.get(i);
      Type desiredType = null;
      if (storageTypes != null) {
        desiredType = storageTypes.get(i);
      }
      final Expression translate = translate(rex, desiredType);
      list.add(translate);
      // desiredType is still a hint, thus we might get any kind of output
      // (boxed or not) when hint was provided.
      // It is favourable to get the type matching desired type
      if (desiredType == null && !isNullable(rex)) {
        assert !Primitive.isBox(translate.getType())
            : "Not-null boxed primitive should come back as primitive: "
            + rex + ", " + translate.getType();
      }
    }
    return list;

  }

  public boolean isNullable(RexNode e) {
    return e.getType().isNullable();
  }

  /** Translates an expression that is not in the cache.
   *
   * @param expr Expression
   * @param nullAs If false, if expression is definitely not null at
   *   runtime. Therefore we can optimize. For example, we can cast to int
   *   using x.intValue().
   * @return Translated expression
   */
  private Expression translate0(RexNode expr, RexImpTable.NullAs nullAs,
      Type storageType) {
    if (nullAs == RexImpTable.NullAs.NULL && !expr.getType().isNullable()) {
      nullAs = RexImpTable.NullAs.NOT_POSSIBLE;
    }
    switch (expr.getKind()) {
      case INPUT_REF: {
        final int index = ((RexInputRef) expr).getIndex();
        Expression x = inputGetter.field(list, index, storageType);

        Expression input = list.append("inp" + index + "_", x); // safe to share
        return handleNullUnboxingIfNecessary(input, nullAs, storageType);
      }
      case LOCAL_REF:
        return translate(
            deref(expr),
            nullAs,
            storageType);
      case LITERAL:
        return translateLiteral(
            (RexLiteral) expr,
            nullifyType(
                expr.getType(),
                isNullable(expr)
                    && nullAs != RexImpTable.NullAs.NOT_POSSIBLE),
            typeFactory,
            nullAs);
      // TODO: Do we need dynamic param in BeamSQL?
      case DYNAMIC_PARAM:
      //   return translateParameter((RexDynamicParam) expr, nullAs, storageType);
        throw new RuntimeException("Cannot translate " + expr + " in code generation");
      case CORREL_VARIABLE:
        throw new RuntimeException("Cannot translate " + expr + ". Correlated"
            + " variables should always be referenced by field access");
      case FIELD_ACCESS: {
        RexFieldAccess fieldAccess = (RexFieldAccess) expr;
        RexNode target = deref(fieldAccess.getReferenceExpr());
        int fieldIndex = fieldAccess.getField().getIndex();
        String fieldName = fieldAccess.getField().getName();
        switch (target.getKind()) {
          case CORREL_VARIABLE:
            if (correlates == null) {
              throw new RuntimeException("Cannot translate " + expr + " since "
                  + "correlate variables resolver is not defined");
            }
            InputGetter getter =
                correlates.apply(((RexCorrelVariable) target).getName());
            Expression y = getter.field(list, fieldIndex, storageType);
            Expression input = list.append("corInp" + fieldIndex + "_", y); // safe to share
            return handleNullUnboxingIfNecessary(input, nullAs, storageType);
          default:
            RexNode rxIndex = builder.makeLiteral(fieldIndex, typeFactory.createType(int.class), true);
            RexNode rxName = builder.makeLiteral(fieldName, typeFactory.createType(String.class), true);
            RexCall accessCall = (RexCall) builder.makeCall(
                fieldAccess.getType(),
                SqlStdOperatorTable.STRUCT_ACCESS,
                ImmutableList.of(target, rxIndex, rxName));
            return translateCall(accessCall, nullAs);
        }
      }
      default:
        if (expr instanceof RexCall) {
          return translateCall((RexCall) expr, nullAs);
        }
        throw new RuntimeException(
            "cannot translate expression " + expr);
    }
  }

  public RelDataType nullifyType(RelDataType type, boolean nullable) {
    if (!nullable) {
      final Primitive primitive = javaPrimitive(type);
      if (primitive != null) {
        return typeFactory.createJavaType(primitive.primitiveClass);
      }
    }
    return typeFactory.createTypeWithNullability(type, nullable);
  }

  private Primitive javaPrimitive(RelDataType type) {
    if (type instanceof RelDataTypeFactoryImpl.JavaType) {
      return Primitive.ofBox(
          ((RelDataTypeFactoryImpl.JavaType) type).getJavaClass());
    }
    return null;
  }

  /** Translates a literal.
   *
   * @throws AlwaysNull if literal is null but {@code nullAs} is
   * {@link org.apache.calcite.adapter.enumerable.RexImpTable.NullAs#NOT_POSSIBLE}.
   */
  public static Expression translateLiteral(
      RexLiteral literal,
      RelDataType type,
      JavaTypeFactory typeFactory,
      RexImpTable.NullAs nullAs) {
    if (literal.isNull()) {
      switch (nullAs) {
        case TRUE:
        case IS_NULL:
          return RexImpTable.TRUE_EXPR;
        case FALSE:
        case IS_NOT_NULL:
          return RexImpTable.FALSE_EXPR;
        case NOT_POSSIBLE:
          throw AlwaysNull.INSTANCE;
        case NULL:
        default:
          return RexImpTable.NULL_EXPR;
      }
    } else {
      switch (nullAs) {
        case IS_NOT_NULL:
          return RexImpTable.TRUE_EXPR;
        case IS_NULL:
          return RexImpTable.FALSE_EXPR;
      }
    }
    Type javaClass = typeFactory.getJavaClass(type);
    final Object value2;
    switch (literal.getType().getSqlTypeName()) {
      case DECIMAL:
        final BigDecimal bd = literal.getValueAs(BigDecimal.class);
        if (javaClass == float.class) {
          return Expressions.constant(bd, javaClass);
        } else if (javaClass == double.class) {
          return Expressions.constant(bd, javaClass);
        }
        assert javaClass == BigDecimal.class;
        return Expressions.new_(BigDecimal.class,
            Expressions.constant(bd.toString()));
      case DATE:
      // TODO: handle TIME cast, not to an int.
      case TIME:
      case TIME_WITH_LOCAL_TIME_ZONE:
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
        value2 = literal.getValueAs(Integer.class);
        javaClass = int.class;
        break;
      // TODO: handle TIMESTAMP cast, not to a long.
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
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
        value2 = literal.getValueAs(Long.class);
        javaClass = long.class;
        break;
      case CHAR:
      case VARCHAR:
        value2 = literal.getValueAs(String.class);
        break;
      case BINARY:
      case VARBINARY:
        return Expressions.new_(
            ByteString.class,
            Expressions.constant(
                literal.getValueAs(byte[].class),
                byte[].class));
      // TODO: BeamSQL does not support SYMBOL, remove.
      case SYMBOL:
        value2 = literal.getValueAs(Enum.class);
        javaClass = value2.getClass();
        break;
      // primitive type: int, float, double, etc.
      default:
        final Primitive primitive = Primitive.ofBoxOr(javaClass);
        final Comparable value = literal.getValueAs(Comparable.class);
        if (primitive != null && value instanceof Number) {
          value2 = primitive.number((Number) value);
        } else {
          value2 = value;
        }
    }
    return Expressions.constant(value2, javaClass);
  }

  /** Dereferences an expression if it is a
   * {@link org.apache.calcite.rex.RexLocalRef}. */
  public RexNode deref(RexNode expr) {
    if (expr instanceof RexLocalRef) {
      RexLocalRef ref = (RexLocalRef) expr;
      final RexNode e2 = program.getExprList().get(ref.getIndex());
      assert ref.getType().equals(e2.getType());
      return e2;
    } else {
      return expr;
    }
  }

  /** Thrown in the unusual (but not erroneous) situation where the expression
   * we are translating is the null literal but we have already checked that
   * it is not null. It is easier to throw (and caller will always handle)
   * than to check exhaustively beforehand. */
  static class AlwaysNull extends ControlFlowException {
    @SuppressWarnings("ThrowableInstanceNeverThrown")
    public static final AlwaysNull INSTANCE = new AlwaysNull();

    private AlwaysNull() {}
  }
}
