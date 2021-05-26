using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Phy.EfBulk
{
    public class CustomExpressionVisitor<TExpression> : ExpressionVisitor where TExpression : Expression
    {
        private readonly Func<TExpression, Expression> _visitor;

        public CustomExpressionVisitor(Func<TExpression, Expression> visitor)
        {
            _visitor = visitor;
        }

        public override Expression Visit(Expression expression)
        {
            if (expression is TExpression && _visitor != null)
                expression = _visitor(expression as TExpression);

            return base.Visit(expression);
        }
    }
}
