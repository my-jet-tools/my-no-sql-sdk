use std::str::FromStr;

use types_reader::{EnumCase, TokensObject};

use crate::EnumOfMyNoSqlEntityParameters;

pub fn generate(
    attr: proc_macro2::TokenStream,
    input: proc_macro::TokenStream,
) -> Result<proc_macro::TokenStream, syn::Error> {
    let parameters: TokensObject = attr.try_into()?;

    let parameters = EnumOfMyNoSqlEntityParameters::try_from(&parameters)?;

    let table_name = parameters.table_name;

    let ast: syn::DeriveInput = syn::parse(input).unwrap();
    let enum_name = &ast.ident;
    let enum_cases = EnumCase::read(&ast)?;

    let partition_keys = get_partition_keys(&enum_cases)?;

    let row_keys = get_row_keys(&enum_cases)?;

    let time_stamps = get_timestamps(&enum_cases)?;

    let serialize_cases = get_serialize_cases(&enum_cases)?;

    let deserialize_cases = get_deserialize_cases(&enum_cases)?;

    let unwraps = if parameters.generate_unwraps {
        let unwraps = generate_unwraps(&enum_cases)?;

        quote::quote! {
            impl #enum_name{
                #unwraps
            }
        }
    } else {
        proc_macro2::TokenStream::new()
    };

    let into_s = generate_into_for_each_case(enum_name,&enum_cases)?;
    let result = quote::quote! {
        #ast

        #unwraps

        impl my_no_sql_sdk::abstractions::MyNoSqlEntity for #enum_name {

            const TABLE_NAME: &'static str = #table_name;

            const LAZY_DESERIALIZATION: bool = true;

        fn get_partition_key(&self) -> &str {
            use my_no_sql_sdk::abstractions::MyNoSqlEntity;
            match self {
                #partition_keys
            }
        }

        fn get_row_key(&self) -> &str {
            use my_no_sql_sdk::abstractions::MyNoSqlEntity;
            match self {
                #row_keys
            }
        }

        fn get_time_stamp(&self) -> my_no_sql_sdk::abstractions::Timestamp {
            use my_no_sql_sdk::abstractions::MyNoSqlEntity;
            match self {
                #time_stamps
            }
        }

        

       }

       impl my_no_sql_sdk::abstractions::MyNoSqlEntitySerializer for #enum_name {
        fn serialize_entity(&self) -> Vec<u8> {
            use my_no_sql_sdk::abstractions::MyNoSqlEntity;
            let (result, row_key) = match self{
                #serialize_cases
            };

            my_no_sql_sdk::core::entity_serializer::inject_partition_key_and_row_key(result, self.get_partition_key(), row_key)

        }
        fn deserialize_entity(src: &[u8]) -> Result<Self, String> {
            #deserialize_cases
        }
    }
       

       #into_s

    };

    Ok(result.into())
}

fn get_partition_keys(enum_cases: &[EnumCase]) -> Result<proc_macro2::TokenStream, syn::Error> {
    let mut result = Vec::new();

    for enum_case in enum_cases {
        let enum_case_ident = enum_case.get_name_ident();

        result.extend(quote::quote! {
            Self::#enum_case_ident(model) => model.get_partition_key(),
        });
    }

    Ok(quote::quote!(#(#result)*))
}

fn get_row_keys(enum_cases: &[EnumCase]) -> Result<proc_macro2::TokenStream, syn::Error> {
    let mut result = Vec::new();

    for enum_case in enum_cases {
        let enum_case_ident = enum_case.get_name_ident();

        result.extend(quote::quote! {
            Self::#enum_case_ident(model) => model.get_row_key(),
        });
    }

    Ok(quote::quote!(#(#result)*))
}

fn get_timestamps(enum_cases: &[EnumCase]) -> Result<proc_macro2::TokenStream, syn::Error> {
    let mut result = Vec::new();

    for enum_case in enum_cases {
        let enum_case_ident = enum_case.get_name_ident();

        result.extend(quote::quote! {
            Self::#enum_case_ident(model) => model.get_time_stamp(),
        });
    }

    Ok(quote::quote!(#(#result)*))
}

fn get_serialize_cases(enum_cases: &[EnumCase]) -> Result<proc_macro2::TokenStream, syn::Error> {
    let mut result = Vec::new();

    for enum_case in enum_cases {
        let enum_case_ident = enum_case.get_name_ident();

        let model = enum_case.model.as_ref().unwrap();
        let model_ident = model.get_name_ident();

        result.extend(quote::quote! {
            Self::#enum_case_ident(model) => (model.serialize_entity(), #model_ident::ROW_KEY),
        });
    }

    Ok(quote::quote!(#(#result)*))
}

fn get_deserialize_cases(enum_cases: &[EnumCase]) -> Result<proc_macro2::TokenStream, syn::Error> {
    let mut result = Vec::new();

    result.push(quote::quote! {
        let entity = my_no_sql_sdk::core::db_json_entity::DbJsonEntity::from_slice(src).unwrap();

        // Which case this is, is a question about the keys - answered against the payload
        // as it lies, without building the keys as strings
        let entity_partition_key = entity.partition_key_value(src);
        let entity_row_key = entity.row_key_value(src);
    });

    for enum_case in enum_cases {
        let enum_case_ident = enum_case.get_name_ident();

        match enum_case.model.as_ref() {
            Some(model) => {
                let model_ident = model.get_name_ident();
                result.push(quote::quote! {

                    if let Some(row_key) = #model_ident::ROW_KEY{
                        if entity_partition_key.eq_with_str(#model_ident::PARTITION_KEY) && entity_row_key.eq_with_str(row_key) {
                            let item = #model_ident::deserialize_entity(src)?;
                            return Ok(Self::#enum_case_ident(item));
                        }
                    }else{
                        if entity_partition_key.eq_with_str(#model_ident::PARTITION_KEY) {
                            let item = #model_ident::deserialize_entity(src)?;
                            return Ok(Self::#enum_case_ident(item));
                        }

                    }
                });
            }
            None => {
                return Err(syn::Error::new_spanned(
                    enum_case_ident,
                    "Enum case must have a model",
                ))
            }
        }
    }

    result.push(quote::quote!{
        use my_no_sql_sdk::abstractions::MyNoSqlEntity;
        Err(format!("Table: '{}'. Unknown Enum Case for the record with PartitionKey: {} and RowKey: {}", Self::TABLE_NAME, entity_partition_key, entity_row_key))
    });

    Ok(quote::quote!(#(#result)*))
}

fn generate_unwraps(enum_cases: &[EnumCase]) -> Result<proc_macro2::TokenStream, syn::Error> {
    let mut result = Vec::new();

    for enum_case in enum_cases {
        if enum_case.model.is_none() {
            continue;
        }

        let enum_model = enum_case.model.as_ref().unwrap();

        let enum_model_name_ident = enum_model.get_name_ident();

        let fn_name = format!(
            "unwrap_{}",
            types_reader::utils::to_snake_case(&enum_case.get_name_ident().to_string())
        );

        let fn_name = proc_macro2::TokenStream::from_str(&fn_name)?;

        let enum_case_ident = enum_case.get_name_ident();

        let enum_case_str = enum_case_ident.to_string();

        result.extend(quote::quote! {
            pub fn #fn_name(&self) -> &#enum_model_name_ident {
                match self {
                    Self::#enum_case_ident(model) => model,
                    _ => panic!("Expected case {}", #enum_case_str)
                }
            }
        });
    }

    Ok(quote::quote!(#(#result)*))
}



fn generate_into_for_each_case(enum_name_ident: &syn::Ident, enum_cases: &[EnumCase])-> Result<proc_macro2::TokenStream, syn::Error>{
    let mut result = Vec::new();

    for enum_case in enum_cases{

        if let Some(model) = enum_case.model.as_ref(){

            let model_ident = model.get_name_ident();
            let enum_case_ident = enum_case.get_name_ident();
            let enum_case_str = enum_case_ident.to_string();

            result.push(quote::quote!{
                impl From<#enum_name_ident> for #model_ident{
                    fn from(item: #enum_name_ident) -> Self {
                       match item{
                            #enum_name_ident::#enum_case_ident(model) => model,
                            _ => panic!("Expected case {}", #enum_case_str)
                       }
                    }
                }

            
                impl From<std::sync::Arc<#enum_name_ident>> for #model_ident{
                    fn from(item: std::sync::Arc<#enum_name_ident>) -> Self {
                       match item.as_ref(){
                            #enum_name_ident::#enum_case_ident(model) => model.clone(),
                            _ => panic!("Expected case {}", #enum_case_str)
                       }
        }
    }

            });
    
        }

    }


    Ok(quote::quote!(#(#result)*))
}


