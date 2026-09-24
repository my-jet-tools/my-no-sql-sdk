use std::collections::HashSet;

use quote::{quote, ToTokens};
use syn::Ident;
use types_reader::StructProperty;

pub fn compile_src_with_new_fields(
    ast: &proc_macro2::TokenStream,
    add_pk: bool,
    add_rk: bool,
    add_timestamp: bool,
) -> (Ident, proc_macro2::TokenStream) {
    let mut result: Vec<proc_macro2::TokenTree> = Vec::new();
    let mut struct_name = None;
    let mut passed_struct_name = false;

    for item in ast.into_token_stream() {
        if struct_name.is_none() {
            if let proc_macro2::TokenTree::Ident(ident) = &item {
                if passed_struct_name {
                    struct_name = Some(ident.clone());
                } else {
                    if ident.to_string() == "struct" {
                        passed_struct_name = true;
                    }
                }
            }
            result.push(item);
        } else {
            if let proc_macro2::TokenTree::Group(group) = &item {
                if group.delimiter() == proc_macro2::Delimiter::Brace {
                    let mut first = true;

                    let mut result_tokens: Vec<proc_macro2::TokenTree> = Vec::new();

                    for token in group.stream() {
                        if first {
                            populate_tokens(&mut result_tokens, add_pk, add_rk, add_timestamp);
                            first = false;
                        }
                        result_tokens.push(token);
                    }

                    if result_tokens.len() == 0 {
                        populate_tokens(&mut result_tokens, add_pk, add_rk, add_timestamp);
                    }

                    result.push(proc_macro2::TokenTree::Group(proc_macro2::Group::new(
                        proc_macro2::Delimiter::Brace,
                        result_tokens.into_iter().collect(),
                    )));
                }
            }
        }
    }

    let struct_name = struct_name.unwrap();
    (struct_name, quote::quote!(#(#result)*))
}

pub fn extract_attributes(ast: &proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    let mut derive_result = Vec::new();

    for item in ast.into_token_stream() {
        if let proc_macro2::TokenTree::Ident(item) = &item {
            let to_str = item.to_string();
            if to_str == "pub" || to_str == "struct" {
                break;
            }
        }

        derive_result.push(item);
    }

    quote! {#(#derive_result)*}
}

pub fn compile_struct_with_new_fields(
    struct_name: &Ident,
    derive: proc_macro2::TokenStream,
    fields: &[StructProperty],
    render_expires: bool,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let mut structure_fields = Vec::new();

    let mut serde_fields = HashSet::new();
    serde_fields.insert("PartitionKey");
    serde_fields.insert("RowKey");
    serde_fields.insert("TimeStamp");

    for field in fields {
        if field.name == "expires" {
            if !field.ty.as_str().as_str().ends_with("Timestamp") {
                return field.throw_error("Field must be a Timestamp");
            }
        }

        if let Some(rename_attr) = field.attrs.try_get_attr("serde") {
            let param_rename = rename_attr.try_get_named_param("rename");
            if let Some(param_rename) = param_rename {
                let param_rename = param_rename.unwrap_any_value_as_str()?;
                let param_rename = param_rename.as_str()?;

                if serde_fields.contains(param_rename) {
                    return field.throw_error("Field with the same Serde name exists");
                }

                serde_fields.insert(param_rename);
            }
        }

        if serde_fields.contains(field.name.as_str()) {
            return field.throw_error("Field with the same Serde name exists");
        }

        serde_fields.insert(field.name.as_str());

        let field = field.field;

        structure_fields.push(quote::quote! {#field,});
    }

    if render_expires {
        structure_fields.push(quote::quote! {
            #[serde(rename="Expires")]
            pub expires: my_no_sql_sdk::abstractions::Timestamp,
        });
    }

    // #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    let result = quote! {

        #derive
        pub struct #struct_name{
            #[serde(rename="PartitionKey")]
            pub partition_key: String,
            #[serde(rename="RowKey")]
            pub row_key: String,
            #[serde(rename="TimeStamp")]
            #[serde(skip_serializing_if = "my_no_sql_sdk::abstractions::skip_timestamp_serializing")]
            pub time_stamp: my_no_sql_sdk::abstractions::Timestamp,
            #(#structure_fields)*
        }
    };

    Ok(result)
}

fn populate_tokens(
    result_tokens: &mut Vec<proc_macro2::TokenTree>,
    add_pk: bool,
    add_rk: bool,
    add_timestamp: bool,
) {
    if add_pk {
        result_tokens.extend(get_partition_key_token());
    }

    if add_rk {
        result_tokens.extend(get_row_key_token());
    }

    if add_timestamp {
        result_tokens.extend(get_time_stamp_token());
    }
}

pub fn get_partition_key_token() -> proc_macro2::TokenStream {
    quote::quote! {
        #[serde(rename = "PartitionKey")]
        pub partition_key: String,
    }
}

pub fn get_row_key_token() -> proc_macro2::TokenStream {
    quote::quote! {
        #[serde(rename = "RowKey")]
        pub row_key: String,
    }
}
pub fn get_time_stamp_token() -> proc_macro2::TokenStream {
    quote::quote! {
        #[serde(rename = "TimeStamp")]
        pub time_stamp: my_no_sql_sdk::abstractions::Timestamp,
    }
}

pub fn get_fn_get_time_stamp_token() -> proc_macro2::TokenStream {
    quote::quote! {
        fn get_time_stamp(&self) -> my_no_sql_sdk::abstractions::Timestamp {
            self.time_stamp
        }
    }
}

pub fn get_fn_standard_serialize_deserialize() -> proc_macro2::TokenStream {
    quote::quote! {
        fn serialize_entity(&self) -> Vec<u8> {
            my_no_sql_sdk::core::entity_serializer::serialize(self)
        }


        fn deserialize_entity(src: &[u8]) -> Result<Self, String> {
          my_no_sql_sdk::core::entity_serializer::deserialize(src)
        }
    }
}
