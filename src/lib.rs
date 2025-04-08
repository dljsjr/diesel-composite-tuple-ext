use diesel::{prelude::*, query_builder::QueryFragment, sql_types::HasSqlType};

mod private {
    use std::{fmt::Debug, marker::PhantomData};

    use diesel::{
        backend::Backend,
        expression::ValidGrouping,
        prelude::*,
        query_builder::{AstPass, QueryFragment, QueryId},
        sql_types::HasSqlType,
    };

    pub trait SqlTuple<DB: diesel::backend::Backend, T, TSql> {
        fn handle_tuple_fragment<'b>(
            &'b self,
            out: diesel::query_builder::AstPass<'_, 'b, DB>,
        ) -> QueryResult<()>;
    }

    #[derive(Debug, Clone)]
    pub struct Tuples<DB, T, TSql, W>
    where
        DB: diesel::backend::Backend,
        T: Debug,
        TSql: Debug,
        W: SqlTuple<DB, T, TSql>,
    {
        /// The values contained in the `IN (values)` clause
        pub values: Vec<W>,
        pub _t: PhantomData<T>,
        pub _tsql: PhantomData<TSql>,
        pub _db: PhantomData<DB>,
    }

    impl<DB, T, TSql, W> QueryFragment<DB> for Tuples<DB, T, TSql, W>
    where
        DB: Backend + HasSqlType<TSql>,
        T: Debug,
        TSql: Debug,
        W: SqlTuple<DB, T, TSql>,
    {
        fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> {
            out.unsafe_to_cache_prepared();
            let mut first = true;
            for value in &self.values {
                if first {
                    first = false;
                } else {
                    out.push_sql(", ");
                }
                value.handle_tuple_fragment(out.reborrow())?;
            }
            Ok(())
        }
    }

    pub struct InTuples<DB, Ck, T, TSql, W>
    where
        DB: Backend + HasSqlType<TSql>,
        T: Debug,
        TSql: Debug,
        W: SqlTuple<DB, T, TSql>,
        Ck: super::CompositeKey<DB>,
    {
        pub left: Ck,
        pub values: Tuples<DB, T, TSql, W>,
    }

    impl<DB, GB, Ck, T, TSql, W> ValidGrouping<GB> for InTuples<DB, Ck, T, TSql, W>
    where
        DB: Backend + HasSqlType<TSql>,
        T: Debug,
        TSql: Debug,
        W: SqlTuple<DB, T, TSql>,
        Ck: super::CompositeKey<DB>,
    {
        type IsAggregate = diesel::expression::is_aggregate::No;
    }

    impl<DB, Ck, T, TSql, W> Expression for InTuples<DB, Ck, T, TSql, W>
    where
        DB: diesel::backend::Backend + HasSqlType<TSql>,
        T: Debug,
        TSql: Debug + diesel::sql_types::SqlType,
        diesel::sql_types::is_nullable::IsSqlTypeNullable<TSql>:
            diesel::sql_types::MaybeNullableType<diesel::sql_types::Bool>,
        W: SqlTuple<DB, T, TSql>,
        Ck: super::CompositeKey<DB>,
    {
        type SqlType = diesel::sql_types::is_nullable::MaybeNullable<
            diesel::sql_types::is_nullable::IsSqlTypeNullable<TSql>,
            diesel::sql_types::Bool,
        >;
    }

    impl<DB, Ck, T, TSql, W> QueryFragment<DB> for InTuples<DB, Ck, T, TSql, W>
    where
        T: Debug,
        TSql: Debug,
        W: SqlTuple<DB, T, TSql>,
        DB: Backend + HasSqlType<TSql>,
        Ck: super::CompositeKey<DB>,
    {
        fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> {
            out.push_sql("(");
            self.left.walk_ast(out.reborrow())?;
            out.push_sql(")");
            out.push_sql(" IN (VALUES ");
            self.values.walk_ast(out.reborrow())?;
            out.push_sql(")");
            Ok(())
        }
    }

    impl<DB, Ck, T, TSql, W> QueryId for InTuples<DB, Ck, T, TSql, W>
    where
        T: Debug,
        TSql: Debug,
        W: SqlTuple<DB, T, TSql>,
        DB: Backend + HasSqlType<TSql>,
        Ck: super::CompositeKey<DB>,
    {
        type QueryId = ();
        const HAS_STATIC_QUERY_ID: bool = false;
    }

    impl<QS, DB: diesel::backend::Backend, Ck, T, TSql, W> AppearsOnTable<QS>
        for InTuples<DB, Ck, T, TSql, W>
    where
        InTuples<DB, Ck, T, TSql, W>: Expression,
        T: Debug,
        TSql: Debug + diesel::sql_types::SqlType,
        W: SqlTuple<DB, T, TSql>,
        DB: Backend + HasSqlType<TSql>,
        Ck: super::CompositeKey<DB> + AppearsOnTable<QS>,
        <TSql as diesel::sql_types::SqlType>::IsNull:
            diesel::sql_types::MaybeNullableType<diesel::sql_types::Bool>,
    {
    }

    impl<QS, DB: diesel::backend::Backend, Ck, T, TSql, W> SelectableExpression<QS>
        for InTuples<DB, Ck, T, TSql, W>
    where
        InTuples<DB, Ck, T, TSql, W>: AppearsOnTable<QS>,
        T: Debug,
        TSql: Debug + diesel::sql_types::SqlType + SelectableExpression<QS>,
        W: SqlTuple<DB, T, TSql>,
        DB: Backend + HasSqlType<TSql>,
        Ck: super::CompositeKey<DB> + SelectableExpression<QS>,
        <TSql as diesel::sql_types::SqlType>::IsNull:
            diesel::sql_types::MaybeNullableType<diesel::sql_types::Bool>,
    {
    }
}

/// A type that expresses a query to look for a tuple in a list of tuples,
/// or put another way, it performs `WHERE (Lhs) IN (VALUES <RHS...>)`.
pub type IsInTupleList<DB, Lhs, Rhs> =
    private::InTuples<DB, Lhs, Rhs, <Lhs as Expression>::SqlType, Rhs>;

/// Types that behave as composite keys in a query. Can be applied to composite
/// primary keys, but it can also be applied to any time that you want to use a
/// tuple as the left-hand side of a WHERE clause.
pub trait CompositeKey<DB: diesel::backend::Backend>: QueryFragment<DB> {
    /// Constructs a WHERE clase of the form `WHERE <self> IN (VALUES (values[0]), (values[1]), ... (values[2]))`
    fn is_in<T>(self, values: impl IntoIterator<Item = T>) -> IsInTupleList<DB, Self, T>
    where
        Self: QueryFragment<DB> + Expression + Sized,
        Self::SqlType: diesel::sql_types::SqlType + std::fmt::Debug,
        T: private::SqlTuple<DB, T, Self::SqlType> + std::fmt::Debug,
        DB: diesel::backend::Backend + HasSqlType<Self::SqlType>,
    {
        private::InTuples {
            left: self,
            values: private::Tuples {
                values: values.into_iter().collect(),
                _t: std::marker::PhantomData,
                _db: std::marker::PhantomData,
                _tsql: std::marker::PhantomData,
            },
        }
    }
}

macro_rules! impl_ck {
    ($($t:tt),+) => {
        impl<DB: diesel::backend::Backend, $($t,)+> CompositeKey<DB> for ($($t,)+)
        where
            $($t: diesel::query_builder::QueryFragment<DB>,)+
        {
        }
    };
}

macro_rules! impl_tuple {
  (($first_in:ident, $($in_types:tt),+), ($first_out:ident, $($out_types:tt),+)) => {
        impl<DB: diesel::backend::Backend, $first_in, $($in_types),+, $first_out, $($out_types),+> private::SqlTuple<DB, ($first_in, $($in_types),+), ($first_out, $($out_types),+)> for ($first_in, $($in_types),+)
        where
            $first_in: diesel::expression::AsExpression<$first_out> + diesel::serialize::ToSql<$first_out, DB>,
            $($in_types: diesel::expression::AsExpression<$out_types> + diesel::serialize::ToSql<$out_types, DB>,)+
            $first_out: diesel::expression::TypedExpressionType + diesel::sql_types::SqlType + diesel::sql_types::SingleValue,
            $($out_types: diesel::expression::TypedExpressionType + diesel::sql_types::SqlType + diesel::sql_types::SingleValue,)+
            DB: diesel::backend::Backend + diesel::sql_types::HasSqlType<$first_out> $(+ diesel::sql_types::HasSqlType<$out_types>)+,
        {
          fn handle_tuple_fragment<'b>(
            &'b self,
            mut out: diesel::query_builder::AstPass<'_, 'b, DB>,
          ) -> QueryResult<()> {
            out.push_sql("(");
            #[allow(non_snake_case)]
            let ($first_in, $($in_types,)*) = &self;
            out.push_bind_param($first_in)?;
            $(
              out.push_sql(",");
              out.push_bind_param($in_types)?;
            )*
            out.push_sql(")");
            Ok(())
          }
        }
    };
}

impl_ck!(T0, T1);
impl_ck!(T0, T1, T2);
impl_ck!(T0, T1, T2, T3);
impl_ck!(T0, T1, T2, T3, T4);
impl_ck!(T0, T1, T2, T3, T4, T5);
impl_ck!(T0, T1, T2, T3, T4, T5, T6);
impl_ck!(T0, T1, T2, T3, T4, T5, T6, T7);
impl_ck!(T0, T1, T2, T3, T4, T5, T6, T7, T8);
impl_ck!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_ck!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_ck!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_ck!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);

impl_tuple!((T0, T1), (T0Sql, T1Sql));
impl_tuple!((T0, T1, T2), (T0Sql, T1Sql, T2Sql));
impl_tuple!((T0, T1, T2, T3), (T0Sql, T1Sql, T2Sql, T3Sql));
impl_tuple!((T0, T1, T2, T3, T4), (T0Sql, T1Sql, T2Sql, T3Sql, T4Sql));
impl_tuple!(
    (T0, T1, T2, T3, T4, T5),
    (T0Sql, T1Sql, T2Sql, T3Sql, T4Sql, T5Sql)
);
impl_tuple!(
    (T0, T1, T2, T3, T4, T5, T6),
    (T0Sql, T1Sql, T2Sql, T3Sql, T4Sql, T5Sql, T6Sql)
);
impl_tuple!(
    (T0, T1, T2, T3, T4, T5, T6, T7),
    (T0Sql, T1Sql, T2Sql, T3Sql, T4Sql, T5Sql, T6Sql, T7Sql)
);
impl_tuple!(
    (T0, T1, T2, T3, T4, T5, T6, T7, T8),
    (
        T0Sql, T1Sql, T2Sql, T3Sql, T4Sql, T5Sql, T6Sql, T7Sql, T8Sql
    )
);
impl_tuple!(
    (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9),
    (
        T0Sql, T1Sql, T2Sql, T3Sql, T4Sql, T5Sql, T6Sql, T7Sql, T8Sql, T9Sql
    )
);
impl_tuple!(
    (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10),
    (
        T0Sql, T1Sql, T2Sql, T3Sql, T4Sql, T5Sql, T6Sql, T7Sql, T8Sql, T9Sql, T10Sql
    )
);
impl_tuple!(
    (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11),
    (
        T0Sql, T1Sql, T2Sql, T3Sql, T4Sql, T5Sql, T6Sql, T7Sql, T8Sql, T9Sql, T10Sql, T11Sql
    )
);
impl_tuple!(
    (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12),
    (
        T0Sql, T1Sql, T2Sql, T3Sql, T4Sql, T5Sql, T6Sql, T7Sql, T8Sql, T9Sql, T10Sql, T11Sql,
        T12Sql
    )
);
