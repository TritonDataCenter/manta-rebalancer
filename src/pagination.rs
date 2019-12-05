use diesel::prelude::*;
use diesel::query_dsl::methods::LoadQuery;
use diesel::query_builder::*;
use diesel::pg::Pg;
use diesel::sql_types::BigInt;

pub trait Paginate: Sized {
    fn paginate(self, page: i64) -> Paginated<Self>;
}

impl<T> Paginate for T {
    fn paginate(self, offset: i64) -> Paginated<Self> {
        Paginated {
            query: self,
            limit: DEFAULT_LIMIT,
            offset,
        }
    }
}

const DEFAULT_LIMIT: i64 = 10;

#[derive(Debug, Clone, Copy, QueryId)]
pub struct Paginated<T> {
    query: T,
    offset: i64,
    limit: i64,
}

impl<T> Paginated<T> {
    pub fn limit(self, limit: i64) -> Self {
        Paginated { limit, ..self }
    }

    pub fn load_and_count_pages<U>(self, conn: &PgConnection) -> QueryResult<(Vec<U>, i64)>
        where
            Self: LoadQuery<PgConnection, (U, i64)>,
    {
        let per_page = self.limit;
        let offset = self.offset;
        let results = self.load::<(U, i64)>(conn)?;
        let total = results.get(0).map(|x| x.1).unwrap_or(0);
        let records:Vec<U> = results.into_iter().map(|x| x.0).collect();
        let total_pages = (total as f64 / per_page as f64).ceil() as i64;
        let remaining = total - offset - records.len() as i64;
        Ok((records, remaining))
    }
}

impl<T: Query> Query for Paginated<T> {
    type SqlType = (T::SqlType, BigInt);
}

impl<T> RunQueryDsl<PgConnection> for Paginated<T> {}

impl<T> QueryFragment<Pg> for Paginated<T>
    where
        T: QueryFragment<Pg>,
{
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.push_sql("SELECT *, COUNT(*) OVER () FROM (");
        self.query.walk_ast(out.reborrow())?;
        out.push_sql(") t LIMIT ");
        //out.push_bind_param::<BigInt, _>(&self.per_page)?;
        out.push_bind_param::<BigInt, _>(&self.limit)?;
        out.push_sql(" OFFSET ");
        //let offset = (self.page - 1) * self.per_page;
        //out.push_bind_param::<BigInt, _>(&offset)?;
        out.push_bind_param::<BigInt, _>(&self.offset)?;
        Ok(())
    }
}
