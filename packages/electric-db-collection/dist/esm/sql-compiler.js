import { serialize } from "./pg-serializer.js";
function compileSQL(options, compileSQLOptions) {
  const { where, orderBy, limit } = options;
  const encode = compileSQLOptions?.encode;
  const params = [];
  const compiledSQL = { params };
  if (where) {
    compiledSQL.where = compileBasicExpression(where, params, encode);
  }
  if (orderBy) {
    compiledSQL.orderBy = compileOrderBy(orderBy, params, encode);
  }
  if (limit) {
    compiledSQL.limit = limit;
  }
  if (!where) {
    compiledSQL.where = `true = true`;
  }
  const paramsRecord = params.reduce(
    (acc, param, index) => {
      const serialized = serialize(param);
      if (serialized !== ``) {
        acc[`${index + 1}`] = serialized;
      }
      return acc;
    },
    {}
  );
  return {
    ...compiledSQL,
    params: paramsRecord
  };
}
function quoteIdentifier(name) {
  return `"${name}"`;
}
function compileBasicExpression(exp, params, encode) {
  switch (exp.type) {
    case `val`:
      params.push(exp.value);
      return `$${params.length}`;
    case `ref`: {
      if (exp.path.length !== 1) {
        throw new Error(
          `Compiler can't handle nested properties: ${exp.path.join(`.`)}`
        );
      }
      const columnName = encode ? encode(exp.path[0]) : exp.path[0];
      return quoteIdentifier(columnName);
    }
    case `func`:
      return compileFunction(exp, params, encode);
    default:
      throw new Error(`Unknown expression type`);
  }
}
function compileOrderBy(orderBy, params, encode) {
  const compiledOrderByClauses = orderBy.map(
    (clause) => compileOrderByClause(clause, params, encode)
  );
  return compiledOrderByClauses.join(`,`);
}
function compileOrderByClause(clause, params, encode) {
  const { expression, compareOptions } = clause;
  let sql = compileBasicExpression(expression, params, encode);
  if (compareOptions.direction === `desc`) {
    sql = `${sql} DESC`;
  }
  if (compareOptions.nulls === `first`) {
    sql = `${sql} NULLS FIRST`;
  }
  if (compareOptions.nulls === `last`) {
    sql = `${sql} NULLS LAST`;
  }
  return sql;
}
function compileFunction(exp, params = [], encode) {
  const { name, args } = exp;
  const opName = getOpName(name);
  const compiledArgs = args.map(
    (arg) => compileBasicExpression(arg, params, encode)
  );
  if (name === `isNull` || name === `isUndefined`) {
    if (compiledArgs.length !== 1) {
      throw new Error(`${name} expects 1 argument`);
    }
    return `${compiledArgs[0]} ${opName}`;
  }
  if (name === `not`) {
    if (compiledArgs.length !== 1) {
      throw new Error(`NOT expects 1 argument`);
    }
    const arg = args[0];
    if (arg && arg.type === `func`) {
      const funcArg = arg;
      if (funcArg.name === `isNull` || funcArg.name === `isUndefined`) {
        const innerArg = compileBasicExpression(funcArg.args[0], params, encode);
        return `${innerArg} IS NOT NULL`;
      }
    }
    return `${opName} (${compiledArgs[0]})`;
  }
  if (isBinaryOp(name)) {
    if ((name === `and` || name === `or`) && compiledArgs.length > 2) {
      return compiledArgs.map((arg) => `(${arg})`).join(` ${opName} `);
    }
    if (compiledArgs.length !== 2) {
      throw new Error(`Binary operator ${name} expects 2 arguments`);
    }
    const [lhs, rhs] = compiledArgs;
    if (isComparisonOp(name)) {
      const lhsArg = args[0];
      const rhsArg = args[1];
      if (rhsArg && rhsArg.type === `val` && typeof rhsArg.value === `boolean`) {
        const boolValue = rhsArg.value;
        params.pop();
        if (name === `lt`) {
          if (boolValue === true) {
            params.push(false);
            return `${lhs} = $${params.length}`;
          } else {
            return `false`;
          }
        } else if (name === `gt`) {
          if (boolValue === false) {
            params.push(true);
            return `${lhs} = $${params.length}`;
          } else {
            return `false`;
          }
        } else if (name === `lte`) {
          if (boolValue === true) {
            return `true`;
          } else {
            params.push(false);
            return `${lhs} = $${params.length}`;
          }
        } else if (name === `gte`) {
          if (boolValue === false) {
            return `true`;
          } else {
            params.push(true);
            return `${lhs} = $${params.length}`;
          }
        }
      }
      if (lhsArg && lhsArg.type === `val` && typeof lhsArg.value === `boolean`) {
        const boolValue = lhsArg.value;
        params.pop();
        params.pop();
        const rhsCompiled = compileBasicExpression(rhsArg, params, encode);
        if (name === `lt`) {
          if (boolValue === true) {
            return `false`;
          } else {
            params.push(true);
            return `${rhsCompiled} = $${params.length}`;
          }
        } else if (name === `gt`) {
          if (boolValue === true) {
            params.push(false);
            return `${rhsCompiled} = $${params.length}`;
          } else {
            return `false`;
          }
        } else if (name === `lte`) {
          if (boolValue === false) {
            return `true`;
          } else {
            params.push(true);
            return `${rhsCompiled} = $${params.length}`;
          }
        } else if (name === `gte`) {
          if (boolValue === true) {
            return `true`;
          } else {
            params.push(false);
            return `${rhsCompiled} = $${params.length}`;
          }
        }
      }
    }
    if (name === `in`) {
      return `${lhs} ${opName}(${rhs})`;
    }
    return `${lhs} ${opName} ${rhs}`;
  }
  return `${opName}(${compiledArgs.join(`,`)})`;
}
function isBinaryOp(name) {
  const binaryOps = [
    `eq`,
    `gt`,
    `gte`,
    `lt`,
    `lte`,
    `and`,
    `or`,
    `in`,
    `like`,
    `ilike`
  ];
  return binaryOps.includes(name);
}
function isComparisonOp(name) {
  return [`gt`, `gte`, `lt`, `lte`].includes(name);
}
function getOpName(name) {
  const opNames = {
    eq: `=`,
    gt: `>`,
    gte: `>=`,
    lt: `<`,
    lte: `<=`,
    add: `+`,
    and: `AND`,
    or: `OR`,
    not: `NOT`,
    isUndefined: `IS NULL`,
    isNull: `IS NULL`,
    in: `= ANY`,
    // Use = ANY syntax for array parameters
    like: `LIKE`,
    ilike: `ILIKE`,
    upper: `UPPER`,
    lower: `LOWER`,
    length: `LENGTH`,
    concat: `CONCAT`,
    coalesce: `COALESCE`
  };
  const opName = opNames[name];
  if (!opName) {
    throw new Error(`Unknown operator/function: ${name}`);
  }
  return opName;
}
export {
  compileSQL
};
//# sourceMappingURL=sql-compiler.js.map
