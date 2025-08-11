


# Done ====
proc_munis <- function(data, crs) {
  data |>
    st_preprocess(crs) |>
    dplyr::select(
      id = town_id, 
      name = town
    ) |>
    dplyr::mutate(
      dplyr::across(
        dplyr::where(is.character), 
        stringr::str_to_upper
      ),
      id = stringr::str_pad(id, 3, side="left", pad="0")
    ) |>
    dplyr::mutate(
      name = dplyr::case_when(
        name == "MANCHESTER" ~ "MANCHESTER-BY-THE-SEA",
        .default = name
      ),
      name = stringr::str_replace(name, "BORO$", "BOROUGH")
    ) |>
    dplyr::left_join(
      load_muni_helper() |> 
        dplyr::select(id, hns, mapc),
      by = dplyr::join_by(id == id)
    )
}

proc_zips <- function(data, state, munis, states, crs, thresh = 1) {
  if(!(dplyr::between(thresh, 0, 1) | is.null(thresh))) {
    stop("Threshold must be between 0 and 1, or NULL.")
  }
  
  data <- data |>
    st_preprocess(crs) |>
    dplyr::select(zip = zcta5ce20) |>
    sf::st_transform(5070) |>
    sf::st_set_agr("constant")
  
  states <- states |>
    dplyr::select(state = abbrev) |> 
    sf::st_transform(5070) |>
    sf::st_set_agr("constant")
  
  state_from_zip <- data |>
    sf::st_intersection(states) |> 
    sf::st_drop_geometry() |>
    dplyr::filter(!is.na(state)) |>
    dplyr::mutate(
      state_from = dplyr::case_when(
        dplyr::n() == 1 ~ state,
        .default = NA_character_
      ),
      .by = zip
    ) |>
    dplyr::distinct()
  
  data <- data |>
    dplyr::left_join(
      state_from_zip,
      by = dplyr::join_by(zip)
    )
  
  muni_mask <- munis |>
    sf::st_union()
  
  zips_state <- data  |>
    dplyr::filter(state == state) |>
    sf::st_transform(crs) |>
    sf::st_set_agr("constant") |>
    sf::st_intersection(muni_mask)
  
  if (thresh == 1 | is.null(thresh)) {
    muni_from_zip <- munis |>
      sf::st_join(
        zips_state, 
        join = sf::st_contains, 
        left = FALSE
      ) |>
      sf::st_drop_geometry()
  } else {
    muni_from_zip <- munis |>
      std_calculate_overlap(zips_state)  |>
      dplyr::filter(overlap > thresh)
  }
  
  data |>
    dplyr::left_join(
      muni_from_zip |>
        dplyr::select(zip, muni_from = name) |>
        dplyr::distinct(),
      by=dplyr::join_by(zip)
    ) |>
    dplyr::mutate(
      muni_from = dplyr::case_when(
        is.na(state_from) ~ NA_character_,
        .default = muni_from
      )
    ) |>
    sf::st_as_sf() |>
    sf::st_transform(crs) |>
    dplyr::filter(!is.na(state))
}

proc_parcels <- function(x, cbgs, state, crs) {
  col_list <- load_col_crosswalk("parcels", state = state)
  
  x <- x |>
    load_rename_with_cols(
      from = col_list$from, 
      to = col_list$to
    ) |>
    dplyr::select(col_list$to) |>
    sf::st_cast("MULTIPOLYGON") |>
    st_preprocess(crs) |>
    dplyr::mutate(
      muni_id = stringr::str_pad(muni_id, 3, side="left", pad="0")
    )
  
  polys <- x  |>
    tigris::erase_water(area_threshold = 0.9)
  
  points <- x |>
    sf::st_set_agr("constant") |>
    sf::st_point_on_surface() |>
    sf::st_join(
      cbgs |>
        dplyr::select(cbg_id = id)
    ) |>
    dplyr::mutate(
      ct_id = stringr::str_sub(cbg_id, start = 1L, end = 11L)
    )
  
  list(
    points = points,
    polys = polys |>
      dplyr::left_join(
        points |>
          sf::st_drop_geometry() |>
          dplyr::select(loc_id, cbg_id, ct_id),
        by = dplyr::join_by(loc_id)
      )
  )
}

# In-Progress ====
#' Process OpenCorporates Companies
#'
#' @param path Name of directory containing OpenCorporates data.
#' @param gdb_path Path to collection of MassGIS Parcel GDBs.
#' @param filename Name of file containing companies.
#' 
#' @return A data frame of companies.
#' 
#' @export
proc_companies <- function(data, company_count = Inf) {
  data |>
    dplyr::select(
      # changed. previously company_id
      id = company_number,
      name, 
      # new.
      prev_names= previous_names,
      # new.
      alt_names = current_alternative_legal_name,
      # changed.
      type = company_type, 
      # new.
      retrieved_at,
      # new.
      nonprofit,
      # new.
      inactive,
      # new.
      url = registry_url,
      addr = registered_address.street_address,
      muni = registered_address.locality, 
      state = registered_address.region,
      zip = registered_address.postal_code, 
      country = registered_address.country,
      # new.
      incorporation_date,
      dissolution_date
    )
}

proc_officers <- function(x) {
  x
}

proc_alt_names <- function(x, companies) {
  x
}

# proc_assess <- function(df, 
#                         site_prefix,
#                         own_prefix,
#                         zips,
#                         parcels_point,
#                         places,
#                         state_constraint,
#                         quiet = FALSE) {
#   if(!quiet) {
#     util_log_message("BEGIN ASSESSORS TABLE SEQUENCE", header=TRUE)
#   }
#   
#   places <- places |>
#     dplyr::select(-id)
#   
#   df <- df |>
#     proc_assess_address_text(
#       site_prefix = site_prefix,
#       own_prefix = own_prefix,
#       quiet=quiet
#     )
#   
#   df <- df |>
#     proc_assess_address_addr2(
#       site_prefix = site_prefix,
#       own_prefix = own_prefix,
#       quiet=quiet
#     )
#   
#   df <- df |>
#     proc_assess_address_to_range(
#       site_prefix = site_prefix,
#       own_prefix = own_prefix,
#       quiet=quiet
#     )
#   
#   df <- df |>
#     proc_assess_address_postal(
#       site_prefix=site_prefix,
#       own_prefix=own_prefix,
#       zips=zips,
#       parcels_point=parcels_point,
#       state_constraint=state_constraint,
#       quiet=quiet
#     )
#   
#   df <- df |>
#     proc_assess_address_muni(
#       site_prefix=site_prefix,
#       own_prefix=own_prefix,
#       places=places,
#       zips=zips,
#       quiet=quiet
#     )
# }

proc_assess <- function(x, state) {
  col_list <- load_col_crosswalk("assess", state = state)
  
  x <- x |>
    load_rename_with_cols(
      from = col_list$from, 
      to = col_list$to
    ) |>
    dplyr::select(col_list$to)
}

# Notes ====



load_boston_address_preprocess <- function(df, cols, bos_id='035') {
  df <- df |>
    dplyr::filter(!is.na(street_body) & !is.na(street_full_suffix)) |>
    dplyr::mutate(
      muni = "BOSTON",
      muni_id = bos_id,
      is_range = dplyr::case_when(
        !is.na(is_range) ~ as.logical(is_range),
        .default = FALSE
      ),
      body = dplyr::case_when(
        !is.na(street_body) & !is.na(street_full_suffix) ~ stringr::str_to_upper(stringr::str_c(street_body, street_full_suffix, sep = " ")),
        .default = NA_character_
      ),
      state = "MA",
    ) |>
    dplyr::select(
      addr2 = unit,
      body,
      state,
      muni,
      muni_id,
      postal = zip_code,
      num = street_number,
      start = range_from,
      end = range_to,
      is_range
    ) |>
    load_generic_preprocess(cols) |>
    dplyr::mutate(
      dplyr::across(
        c(start, end),
        ~ dplyr::case_when(
          !is.na(.x) ~ stringr::str_replace_all(
            .,
            " 1 ?\\/ ?2", "\\.5"
          ),
          .default = NA_character_
        )
      ),
      dplyr::across(
        c(start, end),
        ~ dplyr::case_when(
          !is.na(.x) & stringr::str_detect(.x, "[0-9]") ~ as.numeric(stringr::str_remove_all(., "[A-Z]")),
          .default = NA
        )
      ),
      range_fix = dplyr::case_when(
        !is.na(num) & !is_range ~ stringr::str_detect(num, "[0-9\\.]+ ?[A-Z]{0,2} ?- ?[0-9\\.]+ ?[A-Z]{0,1}"),
        .default = FALSE
      ),
      start_temp = dplyr::case_when(
        range_fix & !is.na(num) ~ abs(as.numeric(stringr::str_remove_all(stringr::str_extract(num, "^[0-9\\.]+"), "[A-Z]"))),
        .default = NA
      ),
      end_temp = dplyr::case_when(
        range_fix & !is.na(num) ~ abs(as.numeric(stringr::str_remove_all(stringr::str_extract(num, "(?<=[- ]{1,2})[0-9\\.]+ ?[A-Z]{0,1}(?= ?$)"), "[A-Z]"))),
        .default = NA
      ),
      is_range = dplyr::case_when(
        end_temp > start_temp ~ TRUE,
        .default = is_range
      ),
      start = dplyr::case_when(
        is_range & !(start > 0) ~ start_temp,
        .default = start
      ),
      end = dplyr::case_when(
        is_range & !(end > 0) ~ end_temp,
        .default = end
      ),
      dplyr::across(
        c(start, end),
        ~ dplyr::case_when(
          is.na(start) & !is_range & !is.na(num) ~ abs(as.numeric(stringr::str_remove_all(num, "[A-Z]"))),
          .default = .x
        )
      )
    ) |>
    dplyr::filter(!is.na(start) & !is.na(end)) |>
    dplyr::select(-c(is_range, num, range_fix, start_temp, end_temp))
}

load_nonboston_address_preprocess <- function(df, cols, munis) {
  df |>
    dplyr::filter(!is.na(streetname)) |>
    dplyr::mutate(
      state = "MA",
      start = dplyr::case_when(
        num1_sfx == "1/2" ~ num1 + 0.5,
        .default = num1
      ),
      end = dplyr::case_when(
        num2_sfx == "1/2" ~ num2 + 0.5,
        .default = num2
      ),
      muni_id = stringr::str_pad(addrtwn_id, 3, side="left", pad="0")
    ) |>
    dplyr::left_join(
      munis,
      dplyr::join_by(muni_id)
    ) |>
    dplyr::select(
      body = streetname,
      addr2 = unit,
      state,
      muni,
      postal = zipcode,
      start,
      end,
      muni_id
    ) |>
    load_generic_preprocess(cols) |>
    dplyr::mutate(
      end = dplyr::case_when(
        end == 0 ~ start,
        end <= start ~ start,
        .default = end
      )
    )
}

# load_oc_officer_fix_addresses <- function(df, quiet=FALSE) {
#   
#   if(!quiet) {
#     util_log_message(glue::glue("INPUT/OUTPUT: Fixing officer addresses."))
#   }
#   
#   parsed_or_none <- df |>
#     dplyr::filter(is.na(addr) | !is.na(str)) |>
#     dplyr::select(-addr) |>
#     dplyr::rename(addr = str)
#   
#   df |>
#     dplyr::filter(!is.na(addr) & is.na(str)) |>
#     std_extract_zip("addr", "postal") |>
#     std_street_types("addr") |>
#     dplyr::mutate(
#       addr = stringr::str_replace(addr, " [A-Z]{3}$", ""),
#       addr = stringr::str_replace(addr, "(?<= [A-Z]{2}) [A-Z]{2}$", ""),
#       state = stringr::str_extract(addr, "(?<= |^)[A-Z]{2}$"),
#       addr = stringr::str_replace(addr, paste0("[ \\-]", state, "$"), ""),
#       new_addr = std_extract_address_vector(addr, start = TRUE),
#       addr = stringr::str_replace(addr, paste0("(?<= |^)", new_addr, "([ -]|$)"), ""),
#       muni = stringr::str_extract(addr, "(?<= |^)[A-Z\\s]+$"),
#       addr = stringr::str_replace(addr, paste0("(?<= |^)", muni, "([ -]|$)"), ""),
#       addr = dplyr::case_when(
#         !is.na(addr) & !is.na(new_addr) ~ stringr::str_c(new_addr, addr, sep=" "),
#         !is.na(addr) | addr == "" ~ new_addr,
#         .default = new_addr
#       )
#     ) |>
#     dplyr::select(-c(new_addr, str)) |>
#     dplyr::bind_rows(parsed_or_none)
# }


#' load_oc_officers <- function(path, companies, quiet=FALSE, filename = "officers.csv") {
#'   #' Load OpenCorporates Officers
#'   #' 
#'   #' Load OpenCorporates officers.
#'   #'
#'   #' @param path Name of directory containing OpenCorporates data.
#'   #' @param companies Companies as loaded by `load_companies()`.
#'   #' @param filename Name of file containing officers.
#'   #' 
#'   #' @return A data frame of officers.
#'   #' 
#'   #' @export
#'   
#'   if(!quiet) {
#'     util_log_message(glue::glue("INPUT/OUTPUT: Loading officers from {path}/{filename}."))
#'   }
#'   
#'   readr::read_csv(
#'     file.path(path, filename),
#'     col_select = c(name, position, address.in_full, address.street_address, 
#'                    address.locality, address.region, address.postal_code, 
#'                    address.country, company_number),
#'     col_types = "ccccccccc",
#'     progress = FALSE,
#'     show_col_types = FALSE
#'   ) |>
#'     dplyr::rename(
#'       addr = address.in_full, 
#'       str = address.street_address, 
#'       muni = address.locality,
#'       state = address.region,
#'       postal = address.postal_code,
#'       country = address.country,
#'       company_id = company_number
#'     ) |>
#'     dplyr::semi_join(companies, by = dplyr::join_by(company_id == company_id)) |>
#'     std_replace_newline("addr") |>
#'     load_generic_preprocess(
#'       c("addr", "name", "position", "str", "muni", "state", "postal", "country")
#'     ) |>
#'     dplyr::filter(!is.na(name)) |>
#'     load_oc_officer_fix_addresses(quiet=quiet) |>
#'     std_replace_blank(c("addr", "muni", "state", "postal")) |>
#'     tibble::rowid_to_column("id")
#' }
