use std::error::Error;

use sqlx::Row;

mod basic_io_functions;


pub fn format_insert_query(table_name: &str, indexes: &Vec<String>, values: Vec<String>) -> String {
    let mut query = String::from("INSERT INTO ");

    query.push_str(table_name);
    query.push_str(" (");

    for index in indexes {
        query.push_str(index);
        query.push(',');
    }

    query.pop();
    query.push_str(") ");
    query.push_str("VALUES (");

    for value in values {
        query.push('\'');
        query.push_str(&value);
        query.push('\'');
        query.push_str(",")
    }

    query.pop();
    query.push_str(")");

    query
}


pub async fn insert(table_name: &str, indexes: &Vec<String>, values: Vec<String>, pool: &sqlx::PgPool) -> Result<(), Box<dyn Error>> {
    let query = format_insert_query(table_name, &indexes, values);

    sqlx::query(&query)
        .execute(pool)
        .await?;

    Ok(())
}


pub fn format_update_query(table_name: &str, updates: Vec<(String, String)>, key: (&str, &str)) -> String {
    let mut query = String::from("UPDATE ");
    query.push_str(table_name);
    query.push_str(" SET ");

    for update in updates {
        query.push_str(&update.0);
        query.push_str(" = ");
        query.push('\'');
        query.push_str(&update.1);
        query.push('\'');
        query.push(',')
    }
    query.pop();

    query.push_str(" WHERE ");
    query.push_str(key.0);
    query.push_str(" = ");
    query.push('\'');
    query.push_str(key.1);
    query.push('\'');

    query
}

pub async fn update(table_name: &str, updates: Vec<(String, String)>, key: (&str, &str), pool: &sqlx::PgPool) -> Result<(), Box<dyn Error>> {
    let query = format_update_query(table_name, updates, key);

    sqlx::query(&query)
        .execute(pool)
        .await?;
    
        Ok(())

}

pub fn format_select_string(table_name: &str, fields: &Vec<String>, key: (&str, &str)) -> String {
    let mut query = String::from("SELECT ");

    for field in fields {
        query.push_str(field);
        query.push(',');
    }
    query.pop();

    query.push_str(" FROM ");
    query.push_str(table_name);
    query.push_str(" WHERE ");
    query.push_str(key.0);
    query.push_str(" = ");
    query.push('\'');
    query.push_str(key.1);
    query.push('\'');

    query
}

pub async fn select(table_name: &str, fields: Vec<String>, key: (&str, &str), pool: &sqlx::PgPool) -> Result<Vec<Vec<String>>, Box<dyn Error>> {
    let query = format_select_string(table_name, &fields, key);
    let q = sqlx::query(&query);

    let rows = q.fetch_all(pool).await?;

    let mut output = Vec::new();

    for row in rows {
        let mut inner = Vec::new();
        for field in &fields {
            let input: String = row.get(&field[..]);
            inner.push(input);
        }
        output.push(inner);
    }

    Ok(output)

}

pub async fn insert_transaction(table_name: &str, indexes: &Vec<String>, values: Vec<Vec<String>>, pool: &sqlx::PgPool) -> Result<(), Box<dyn Error>> {
    let mut txn = pool.begin().await?;

    for value in values {
        let query = format_insert_query(table_name, indexes, value);
        sqlx::query(&query)
            .execute(&mut txn)
            .await?;
    }

    txn.commit().await?;

    Ok(())
}


#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[tokio::test]
    async fn test_insert() -> Result<(), Box<dyn Error>>{
        let url = "postgres://halli:halli@localhost:5432/sqlx_test";
        let pool = sqlx::postgres::PgPool::connect(url).await?;

        sqlx::migrate!("./migrations").run(&pool).await?;

        let table_name = "book";
        let indexes = Vec::from(["title".to_owned(), "author".to_owned(), "isbn".to_owned()]);
        let values = Vec::from(["Witcher".to_owned(), "Andrzej Sapkowski".to_owned(), "Some other number".to_owned()]);

        insert(table_name, &indexes, values, &pool).await?;

        Ok(())

    }

    #[tokio::test]
    async fn test_update_string() -> Result<(), Box<dyn Error>> {
        let table_name = "book";
        let updates = Vec::from([("title".to_owned(), "Witcher".to_owned()), ("author".to_owned(), "Andy Sappy".to_owned())]);
        let key = ("isbn", "Some number");

        let query = format_update_query(table_name, updates, key);

        println!("{}", query);
        
        Ok(())
    }

    #[tokio::test]
    async fn test_update_database() -> Result<(), Box<dyn Error>> {
        let url = "postgres://halli:halli@localhost:5432/sqlx_test";
        let pool = sqlx::postgres::PgPool::connect(url).await?;

        let table_name = "book";
        let updates = Vec::from([("title".to_owned(), "Witcher".to_owned()), ("author".to_owned(), "Andy Sappy".to_owned())]);
        let key = ("isbn", "Some number");

        update(table_name, updates, key, &pool).await?;
        
        Ok(())
    }

    #[tokio::test]
    async fn test_select_string() -> Result<(), Box<dyn Error>> {
        let table_name = "book";
        let fields = Vec::from(["title".to_owned(), "author".to_owned(), "isbn".to_owned()]);
        let key = ("isbn", "Some number");

        let query = format_select_string(table_name, &fields, key);

        println!("{}", query);
        
        Ok(())
    }

    #[tokio::test]
    async fn test_select_database() -> Result<(), Box<dyn Error>> {
        let url = "postgres://halli:halli@localhost:5432/sqlx_test";
        let pool = sqlx::postgres::PgPool::connect(url).await?;

        let table_name = "book";
        let fields = Vec::from(["title".to_owned(), "author".to_owned(), "isbn".to_owned()]);
        let key = ("title", "Witcher");
        
        let output = select(table_name, fields, key, &pool).await?;

        println!("{:?}", output);

        Ok(())
    }

    #[tokio::test]
    async fn test_insert_transaction() -> Result<(), Box<dyn Error>> {
        let url = "postgres://halli:halli@localhost:5432/sqlx_test";
        let pool = sqlx::postgres::PgPool::connect(url).await?;

        let table_name = "book";

        let path = Path::new("sample_books.txt");
        let (header, values) = basic_io_functions::read_to_vec(&path, ';');

        insert_transaction(table_name, &header, values, &pool).await?;

        Ok(())
    }


}