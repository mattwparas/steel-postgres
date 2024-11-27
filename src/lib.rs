use std::error::Error;

use abi_stable::std_types::{RSlice, RSliceMut, RVec};
use bytes::BytesMut;
use postgres::{
    types::{FromSql, ToSql},
    Client, GenericClient, NoTls, Statement,
};
use postgres_types::Type;
use steel::{
    rvals::Custom,
    steel_vm::ffi::{FFIArg, FFIModule, FFIValue, RegisterFFIFn},
};

struct PostgresClient {
    client: Client,
}

unsafe impl Send for PostgresClient {}
unsafe impl Sync for PostgresClient {}

enum Argument {
    Bool(bool),
    Number(f64),
    Int(i32),
    String(String),
    Void,
}

type ToSqlSync = dyn ToSql + Sync;

struct DynamicToSqlNoneType;

impl std::fmt::Debug for DynamicToSqlNoneType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DynamicToSqlNoneType")
    }
}

impl ToSql for DynamicToSqlNoneType {
    fn to_sql(
        &self,
        _ty: &postgres_types::Type,
        _out: &mut BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        Ok(postgres_types::IsNull::Yes)
    }

    fn accepts(_ty: &postgres_types::Type) -> bool
    where
        Self: Sized,
    {
        true
    }

    fn to_sql_checked(
        &self,
        _ty: &postgres_types::Type,
        _out: &mut BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn Error + Sync + Send>> {
        Ok(postgres_types::IsNull::Yes)
    }
}

impl PostgresClient {
    pub fn connect(params: String) -> Self {
        Self {
            client: Client::connect(&params, NoTls).unwrap(),
        }
    }

    pub fn batch_execute(&mut self, queries: &str) -> Result<(), PostgresError> {
        Ok(self.client.batch_execute(queries)?)
    }

    pub fn execute(&mut self, query: &str, bindings: FFIArg) -> Result<FFIValue, PostgresError> {
        if let FFIArg::Vector(bindings) = bindings {
            // Why does this not satisfy the borrow checker?
            let converted: Vec<Argument> = bindings
                .iter()
                .map(|arg| match arg {
                    FFIArg::BoolV(b) => Argument::Bool(*b),
                    FFIArg::NumV(n) => Argument::Number(*n),
                    FFIArg::IntV(i) => Argument::Int(*i as _),
                    FFIArg::StringRef(s) => Argument::String(s.to_string()),
                    FFIArg::StringV(s) => Argument::String(s.to_string()),
                    FFIArg::Void => Argument::Void,
                    a => todo!("{:?}", a),
                })
                .collect();

            let references: Vec<&ToSqlSync> = converted
                .iter()
                .map(|arg| -> &ToSqlSync {
                    match arg {
                        Argument::Bool(b) => b,
                        Argument::Number(n) => n,
                        Argument::Int(i) => i,
                        Argument::String(s) => s,
                        Argument::Void => &DynamicToSqlNoneType,
                    }
                })
                .collect();

            Ok(self
                .client
                .execute(query, references.as_slice())
                .map(|x| FFIValue::IntV(x as _))?)
        } else {
            Err(PostgresError::TypeMismatch)
        }
    }

    // Return a raw row, which then will get converted based on
    // type markers?
    pub fn query(&mut self, query: &str) -> Result<FFIValue, PostgresError> {
        let rows = self.client.query(query, &[])?;

        let mut results = RVec::new();

        for row in rows {
            let width = row.len();

            let mut computed_row: RVec<FFIValue> = RVec::with_capacity(width);

            for i in (0..width).into_iter() {
                // Type check the row coming in
                let typ = row.columns()[i].type_().clone();

                match typ {
                    typ if typ == Type::BOOL => {
                        let value = row
                            .get::<_, Option<bool>>(i)
                            .map(|x| FFIValue::BoolV(x.into()))
                            .unwrap_or(FFIValue::Void);

                        computed_row.push(value);
                    }
                    typ if typ == Type::TEXT => {
                        // TODO
                        let value = row
                            .get::<_, Option<String>>(i)
                            .map(|x| FFIValue::StringV(x.into()))
                            .unwrap_or(FFIValue::Void);

                        computed_row.push(value);
                    }
                    typ if typ == Type::BYTEA => {
                        let value = row
                            .get::<_, Option<Vec<u8>>>(i)
                            .map(|x| FFIValue::ByteVector(x.into()))
                            .unwrap_or(FFIValue::Void);

                        computed_row.push(value);
                    }

                    typ if typ == Type::INT2 => {
                        let value = row
                            .get::<_, Option<i16>>(i)
                            .map(|x| FFIValue::IntV(x as _))
                            .unwrap_or(FFIValue::Void);

                        computed_row.push(value);
                    }

                    typ if typ == Type::INT4 => {
                        let value = row
                            .get::<_, Option<i32>>(i)
                            .map(|x| FFIValue::IntV(x as _))
                            .unwrap_or(FFIValue::Void);

                        computed_row.push(value);
                    }

                    typ if typ == Type::INT8 => {
                        let value = row
                            .get::<_, Option<i64>>(i)
                            .map(|x| FFIValue::IntV(x as _))
                            .unwrap_or(FFIValue::Void);

                        computed_row.push(value);
                    }

                    _ => {
                        todo!()
                    }
                }
            }

            results.push(FFIValue::Vector(computed_row));
        }

        Ok(FFIValue::Vector(results))
    }
}

impl Custom for PostgresClient {}

#[allow(dead_code)]
#[derive(Debug)]
enum PostgresError {
    Error(postgres::Error),
    TypeMismatch,
}

impl Custom for PostgresError {}

impl From<postgres::Error> for PostgresError {
    fn from(value: postgres::Error) -> Self {
        Self::Error(value)
    }
}

impl std::fmt::Display for PostgresError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for PostgresError {}

steel::declare_module!(build_module);

pub fn build_module() -> FFIModule {
    let mut module = FFIModule::new("dylib/steel/postgres");

    module
        .register_fn("client/connect", PostgresClient::connect)
        .register_fn("query", PostgresClient::query)
        .register_fn("batch-execute", PostgresClient::batch_execute)
        .register_fn("execute", PostgresClient::execute);
    module
}
