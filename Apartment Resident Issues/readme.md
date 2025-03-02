# Kafka Data Streaming with Faker and Python

This project simulates the generation of various entities (residents, apartments, contractors, third parties, issues, etc.) using the `Faker` library and streams this fake data to different Kafka topics using Python.

## Kafka Topics Used

The data is streamed to different Kafka topics based on the entity or scenario being generated. Below are the topics and their corresponding data:

### 1. **Residents Topic**
   - **Topic Name:** `residents_topic`
   - **Data Sent:** Information about the residents of the apartments, including their name, contact details, and the apartment they live in.
   - **Fields:** `resident_id`, `first_name`, `last_name`, `email`, `phone_number`, `apartment_id`
   
### 2. **Apartments Topic**
   - **Topic Name:** `apartments_topic`
   - **Data Sent:** Information about the apartments, including their block, number, and floor.
   - **Fields:** `apartment_id`, `block`, `apartment_number`, `floor`

### 3. **Contractors Topic**
   - **Topic Name:** `contractors_topic`
   - **Data Sent:** Information about contractors responsible for services like plumbing, electrical work, cleaning, and landscaping.
   - **Fields:** `contractor_id`, `name`, `service_type`, `contact_number`, `email`

### 4. **Third Parties Topic**
   - **Topic Name:** `third_parties_topic`
   - **Data Sent:** Information about third-party service providers such as insurance companies, city utilities, and repair services.
   - **Fields:** `third_party_id`, `name`, `service_type`, `contact_number`, `email`

### 5. **Issues Topic**
   - **Topic Name:** `issues_topic`
   - **Data Sent:** Details about issues reported by residents related to plumbing, electrical, internet, heating, etc.
   - **Fields:** `issue_id`, `resident_id`, `reported_date`, `issue_type`, `description`, `status`, `priority`

### 6. **Issue Assignments Topic**
   - **Topic Name:** `issue_assignments_topic`
   - **Data Sent:** Information on issue assignments, including which contractor and third-party service provider are assigned to resolve a particular issue.
   - **Fields:** `assignment_id`, `issue_id`, `contractor_id`, `third_party_id`, `assigned_date`, `expected_completion`, `actual_completion`, `role`

### 7. **Charges Topic**
   - **Topic Name:** `charges_topic`
   - **Data Sent:** Information on charges associated with resolving issues, including the contractor and third-party service provider involved, the charge amount, and payment status.
   - **Fields:** `charge_id`, `issue_id`, `contractor_id`, `third_party_id`, `charge_amount`, `charge_date`, `payment_status`

This extended schema allows for managing complex issues reported by residents, which may require multiple contractors or third parties, along with tracking progress and managing associated charges.

https://github.com/AzureDataAnalytics/Kafka/issues/15
