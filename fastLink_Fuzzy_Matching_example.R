# Clear the workspace by removing all objects
rm(list = ls())

# Load the pacman package if it is not already installed.
if (!requireNamespace("pacman", quietly = TRUE)) {
  install.packages("pacman")
}

pacman::p_load(
  dplyr,         # Data manipulation and transformation
  data.table,    # Fast data manipulation with data tables
  stringi,       # Character string processing
  lubridate,     # Date and time handling
  janitor,       # Data cleaning and tabulation functions
  parallel,      # Parallel computing
  fastLink,      # Record linkage and deduplication
  stringdist     # String distance computation
)

# Get the number of detected cores minus 1
# Reserve one core for non-computational tasks to help prevent system slowdowns or unresponsiveness
numCores <- parallel::detectCores() - 1

# Example data frames dfA and dfB
# Replace these with your actual data frames

# dfA <- data.frame(
#   FIRST_NAME = c("John", "Mary", "Robert", "Michael", "Jennifer", "David", "Karen", "Maria", "Carlos", "James"),
#   LAST_NAME = c("Smith III", "Johnson!", "Williams123", "Brown", "Jones", "Davis", "Miller", "Garcia", "Martinez", "Andrson"),
#   BIRTH_DATE = c("1981-05-20", "1990-05-15", "1978-12-10", "1985-08-02", "1993-11-25", "1977-03-30", "1988-06-18", "1991-02-05", "1980-09-12", "1982-07-09")
# )
# 
# dfB <- data.frame(
#   RecipientNameFirst = c("John", "Mary", "Robert", "Michael", "Jennifer", "David", "Karenn", "Carloas", "Mariaa", "James"),
#   RecipientNameLast = c("Smith iv", "Brown-", "Williams", "Jones", "John son", "No Name", "Miller", "Martinez", "Garcia", "Anderson"),
#   RecipientDateOfBirth = c("1981-05-21", "1992-09-25", "1978-10-12", "1985-08-02", "1993-11-25", "1977-03-30", "1988-06-18", "1980-09-12", "1991-02-05", "1982-07-09")
# )

# Example data frames dfA and dfB
dfA <- fread(file = 'Y:/Confidential/DCHS/CDE_PCH/WAIIS_COVID/WDRS-IIS Linkage/Testing and Documentation/Fuzzy Match Demo/dfA.csv',
             sep = ",",
             header = T,
             nThread = numCores)

dfB <- fread(file = 'Y:/Confidential/DCHS/CDE_PCH/WAIIS_COVID/WDRS-IIS Linkage/Testing and Documentation/Fuzzy Match Demo/dfB.csv',
             sep = ",",
             header = T,
             nThread = numCores)

#Assign a unique key ID to each row

dfA <- dfA %>%
 mutate(row_idA = paste("dfA_", row_number(), sep = ""))

dfB <- dfB %>%
  mutate(row_idB = paste("dfB_", row_number(), sep = ""))

# Convert the date column to a valid date format.
# Please comment out this section if you are using the example data provided in the R script. The format of DOB is correct already. 

dfA$BIRTH_DATE <- as.Date(dfA$BIRTH_DATE, format = "%m/%d/%Y")
dfB$RecipientDateOfBirth <- as.Date(dfB$RecipientDateOfBirth, format = "%m/%d/%Y")

# Create new date variables by splitting date of birth into three different parts. 
dfA <- dfA %>% mutate(dob_day = as.numeric(day(BIRTH_DATE)),
                      dob_month = as.numeric(month(BIRTH_DATE)),
                      dob_year = as.numeric(year(BIRTH_DATE)),
                      DOB=BIRTH_DATE)

dfB <- dfB %>% mutate(dob_day = as.numeric(day(RecipientDateOfBirth)),
                      dob_month = as.numeric(month(RecipientDateOfBirth)),
                      dob_year = as.numeric(year(RecipientDateOfBirth)),
                      DOB=RecipientDateOfBirth)


# Define a function for data cleaning with additional name removal logic
clean_names <- function(names_column) {
  # Step 0: Convert to uppercase
  names_column_new <- toupper(names_column)
  # Step 1: Remove specified name suffixes
  toRemove <- c(" JR", " SR", " IV", " III", " II")
  for (tR in toRemove) {
    names_column_new <- gsub(tR, "", names_column_new)
  }
  # Step 2: Convert special characters to ASCII equivalents
  names_column_new <- iconv(names_column_new, "latin1", "ASCII//TRANSLIT", sub = "")
  # Step 3: Remove punctuation, digits, and all sapces
  names_column_new <- gsub("[[:punct:][:digit:]][[:space:]]", "", names_column_new)
  # Step 4: Create a new variable with only alphabetic characters
  names_column_new <- gsub("[^[:alpha:]]", "", names_column_new)
  return(names_column_new)
}

# Perform data cleaning on dfA using the clean_names function
dfA <- dfA %>%
  mutate_at(vars(FIRST_NAME, LAST_NAME), list(new = clean_names)) %>%
  mutate(FN = FIRST_NAME_new, LN = LAST_NAME_new)

# Perform data cleaning on dfB using the clean_names function
dfB <- dfB %>%
  mutate_at(vars(RecipientNameFirst, RecipientNameLast), list(new = clean_names)) %>% 
  mutate(FN = RecipientNameFirst_new, LN = RecipientNameLast_new)

# Create no name list
NoNameList <- c(
  "NICKNAME",
  "NOFAMILYNAME",
  "NOFIRSTNAME",
  "NOLASTNAME",
  "NOMIDDLENAME",
  "NONAME",
  "NO",
  "UNKNOWN",
  "UNK",
  "UN",
  "NA"
)

# Blank out the names in the data if they match any of the strings in the NoNameList
dfA<- dfA %>%
  mutate(FN = case_when(
         FN %in% NoNameList~ "",
         TRUE ~ FN),
         LN = case_when(
         LN %in% NoNameList ~ "",
         TRUE ~ LN))

dfB <- dfB %>%
  mutate(FN = case_when(
         FN %in% NoNameList~ "",
         TRUE ~ FN),
         LN = case_when(
         LN %in% NoNameList ~ "",
         TRUE ~ LN))


# Delete rows that have missing First Name (FN), Last Name (LN), or Date of Birth (DOB).
dfA <- dfA %>%
  filter(!is.na(FN) & FN != "" &
           !is.na( LN) &  LN != "" &
           !is.na(DOB) )

dfB <- dfB %>%
  filter(!is.na(FN) & FN != "" &
        !is.na( LN) &  LN != "" &
        !is.na(DOB) )


# Exact Matching 
# Exact <- merge(dfA, dfB, by=c("FN","LN","DOB")) 

start_time <- Sys.time()

# Using the fastLink R package for record linkage

test <- fastLink(dfA, dfB,
                 # Specify the variables for comparison
                 varnames = c('FN', 'LN', 'dob_day', 'dob_month', 'dob_year'),
                 # Specify which variables should be compared using string distance
                 stringdist.match = c('FN', 'LN'),
                 # Specify which variables can be partially matched
                 partial.match = c('FN', 'LN'),
                 # Specify which variables should be matched numerically
                 numeric.match = c('dob_day', 'dob_month', 'dob_year'),
                 # Specify the number of CPU cores to utilize (parallel processing). The default value is NULL.
                 n.cores = numCores
)

# summary(test)
# Get fuzzy matches using the results from fastLink
# A threshold of 0.98 is set for match classification
fuzzy_matches <- getMatches(dfA, dfB, fl.out = test, threshold.match = 0.98)

# Record the end time
end_time <- Sys.time()
# Calculate the runtime
runtime <- as.period(interval(start_time, end_time))
# Round the minutes to 1 decimal place
rounded_minutes <- round(as.numeric(runtime, "minutes"), 1)
# Print the runtime
print(paste("Script runtime:", rounded_minutes, "minutes"))

# Create new columns for switched birth dates 
fuzzy_matches$BIRTH_DATE_SWITCHED <- format(as.Date(fuzzy_matches$BIRTH_DATE), "%Y-%d-%m")

# Apply the hamming_distance function
fuzzy_matches <- fuzzy_matches %>% mutate(DOBham1 = stringdist(BIRTH_DATE, RecipientDateOfBirth, method = 'hamming')) 
fuzzy_matches <- fuzzy_matches %>% mutate(DOBham2 = stringdist(BIRTH_DATE_SWITCHED, RecipientDateOfBirth, method = 'hamming')) 

# Include only Date of Birth (DOB) with a Hamming distance of less than 1, or in cases where the Day and Month are switched
fuzzy_matches <- fuzzy_matches %>%
  filter(DOBham1 %in% c(0, 1) | DOBham2 == 0)

#Save the fuzzy matching results
fwrite(fuzzy_matches, "Y:/Confidential/DCHS/CDE_PCH/WAIIS_COVID/WDRS-IIS Linkage/Testing and Documentation/Fuzzy Match Demo/fastLink_fuzzy_matches.csv", sep = ",")
saveRDS(fuzzy_matches, "Y:/Confidential/DCHS/CDE_PCH/WAIIS_COVID/WDRS-IIS Linkage/Testing and Documentation/Fuzzy Match Demo/fastLink_fuzzy_matches.RDS")




