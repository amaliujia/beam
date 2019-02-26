package org.apache.beam.sdk.extensions.sql.codegen;

import org.apache.beam.sdk.extensions.sql.impl.planner.BeamJavaTypeFactory;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class CodeGeneratorTest {
  public RexBuilder rexBuilder = new RexBuilder(BeamJavaTypeFactory.INSTANCE);
  public CodeGenerator codeGenerator = new CodeGenerator();

  @Test
  public void translateResolvedLiteral() {
    RexLiteral booleanLiteral = rexBuilder.makeLiteral(false);
    Assert.assertEquals(Expressions.constant(false), codeGenerator.visitLiteral(booleanLiteral));
  }
}
