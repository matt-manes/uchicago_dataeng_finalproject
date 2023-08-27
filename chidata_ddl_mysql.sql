-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema chidata
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema chidata
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `chidata` DEFAULT CHARACTER SET utf8 ;
USE `chidata` ;

-- -----------------------------------------------------
-- Table `chidata`.`business_addresses`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `chidata`.`business_addresses` ;

CREATE TABLE IF NOT EXISTS `chidata`.`business_addresses` (
  `id` INT NOT NULL,
  `street` VARCHAR(65) NULL,
  `zip` INT NULL,
  `ward` TINYINT(2) NULL,
  `latitude` FLOAT NULL,
  `longitude` FLOAT NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `chidata`.`businesses`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `chidata`.`businesses` ;

CREATE TABLE IF NOT EXISTS `chidata`.`businesses` (
  `account_number` INT NOT NULL,
  `legal_name` VARCHAR(120) NULL,
  `dba` VARCHAR(100) NULL,
  `address_id` INT NULL,
  PRIMARY KEY (`account_number`),
  INDEX `address_id_idx` (`address_id` ASC) VISIBLE,
  CONSTRAINT `businesses_address_id`
    FOREIGN KEY (`address_id`)
    REFERENCES `chidata`.`business_addresses` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `chidata`.`license_codes`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `chidata`.`license_codes` ;

CREATE TABLE IF NOT EXISTS `chidata`.`license_codes` (
  `code` INT NOT NULL,
  `description` VARCHAR(60) NULL,
  PRIMARY KEY (`code`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `chidata`.`business_activities`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `chidata`.`business_activities` ;

CREATE TABLE IF NOT EXISTS `chidata`.`business_activities` (
  `id` INT NOT NULL,
  `description` VARCHAR(200) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `chidata`.`application_types`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `chidata`.`application_types` ;

CREATE TABLE IF NOT EXISTS `chidata`.`application_types` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `type` VARCHAR(6) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `chidata`.`application_payments`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `chidata`.`application_payments` ;

CREATE TABLE IF NOT EXISTS `chidata`.`application_payments` (
  `id` INT NOT NULL,
  `date` DATE NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `chidata`.`license_statuses`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `chidata`.`license_statuses` ;

CREATE TABLE IF NOT EXISTS `chidata`.`license_statuses` (
  `id` INT NOT NULL,
  `status` VARCHAR(3) NULL,
  `description` VARCHAR(21) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `chidata`.`licenses`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `chidata`.`licenses` ;

CREATE TABLE IF NOT EXISTS `chidata`.`licenses` (
  `license_number` INT NOT NULL,
  `account_number` INT NULL,
  `start_date` DATE NULL,
  `expiration_date` DATE NULL,
  `issue_date` DATE NULL,
  `status_id` INT NULL,
  `status_change_date` DATE NULL,
  PRIMARY KEY (`license_number`),
  INDEX `account_number_idx` (`account_number` ASC) VISIBLE,
  INDEX `id_idx` (`status_id` ASC) VISIBLE,
  CONSTRAINT `licenses_account_number`
    FOREIGN KEY (`account_number`)
    REFERENCES `chidata`.`businesses` (`account_number`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `licenses_status_id`
    FOREIGN KEY (`status_id`)
    REFERENCES `chidata`.`license_statuses` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `chidata`.`license_applications`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `chidata`.`license_applications` ;

CREATE TABLE IF NOT EXISTS `chidata`.`license_applications` (
  `id` INT NOT NULL,
  `license_number` INT NULL,
  `license_code` INT NULL,
  `account_number` INT NULL,
  `activity_id` INT NULL,
  `application_type_id` INT NULL,
  `payment_id` INT NULL,
  `created_date` DATE NULL,
  `completed_date` DATE NULL,
  `approval_date` DATE NULL,
  `conditional_approval` CHAR(1) NULL,
  `site_number` INT NULL,
  PRIMARY KEY (`id`),
  INDEX `license_number_idx` (`license_number` ASC) VISIBLE,
  INDEX `license_code_idx` (`license_code` ASC) VISIBLE,
  INDEX `activity_id_idx` (`activity_id` ASC) VISIBLE,
  INDEX `application_type_id_idx` (`application_type_id` ASC) VISIBLE,
  INDEX `payment_id_idx` (`payment_id` ASC) VISIBLE,
  INDEX `account_number_idx` (`account_number` ASC) VISIBLE,
  CONSTRAINT `license_applications_license_number`
    FOREIGN KEY (`license_number`)
    REFERENCES `chidata`.`licenses` (`license_number`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `license_applications_license_code`
    FOREIGN KEY (`license_code`)
    REFERENCES `chidata`.`license_codes` (`code`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `license_applications_account_number`
    FOREIGN KEY (`account_number`)
    REFERENCES `chidata`.`businesses` (`account_number`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `license_applications_activity_id`
    FOREIGN KEY (`activity_id`)
    REFERENCES `chidata`.`business_activities` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `license_applications_application_type_id`
    FOREIGN KEY (`application_type_id`)
    REFERENCES `chidata`.`application_types` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `license_applications_payment_id`
    FOREIGN KEY (`payment_id`)
    REFERENCES `chidata`.`application_payments` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `chidata`.`facility_types`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `chidata`.`facility_types` ;

CREATE TABLE IF NOT EXISTS `chidata`.`facility_types` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(45) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `chidata`.`risk_levels`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `chidata`.`risk_levels` ;

CREATE TABLE IF NOT EXISTS `chidata`.`risk_levels` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(15) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `chidata`.`facility_addresses`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `chidata`.`facility_addresses` ;

CREATE TABLE IF NOT EXISTS `chidata`.`facility_addresses` (
  `id` INT NOT NULL,
  `street` VARCHAR(60) NULL,
  `zip` INT NULL,
  `latitude` FLOAT NULL,
  `longitude` FLOAT NULL,
  `facility_type_id` INT NULL,
  `risk_id` INT NULL,
  PRIMARY KEY (`id`),
  INDEX `facility_type_id_idx` (`facility_type_id` ASC) VISIBLE,
  INDEX `risk_id_idx` (`risk_id` ASC) VISIBLE,
  CONSTRAINT `facility_addresses_facility_type_id`
    FOREIGN KEY (`facility_type_id`)
    REFERENCES `chidata`.`facility_types` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `facility_addresses_risk_id`
    FOREIGN KEY (`risk_id`)
    REFERENCES `chidata`.`risk_levels` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `chidata`.`inspected_businesses`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `chidata`.`inspected_businesses` ;

CREATE TABLE IF NOT EXISTS `chidata`.`inspected_businesses` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `license_number` INT NULL,
  `dba` VARCHAR(60) NULL,
  `aka` VARCHAR(60) NULL,
  PRIMARY KEY (`id`),
  INDEX `license_number_idx` (`license_number` ASC) VISIBLE,
  CONSTRAINT `inspected_businesses_license_number`
    FOREIGN KEY (`license_number`)
    REFERENCES `chidata`.`licenses` (`license_number`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `chidata`.`inspection_types`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `chidata`.`inspection_types` ;

CREATE TABLE IF NOT EXISTS `chidata`.`inspection_types` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(40) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `chidata`.`result_types`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `chidata`.`result_types` ;

CREATE TABLE IF NOT EXISTS `chidata`.`result_types` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `description` VARCHAR(20) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `chidata`.`inspections`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `chidata`.`inspections` ;

CREATE TABLE IF NOT EXISTS `chidata`.`inspections` (
  `id` INT NOT NULL,
  `license_number` INT NULL,
  `facility_address_id` INT NULL,
  `inspection_type_id` INT NULL,
  `result_type_id` INT NULL,
  `date` DATE NULL,
  PRIMARY KEY (`id`),
  INDEX `license_number_idx` (`license_number` ASC) VISIBLE,
  INDEX `facility_address_id_idx` (`facility_address_id` ASC) VISIBLE,
  INDEX `inspection_type_id_idx` (`inspection_type_id` ASC) VISIBLE,
  INDEX `result_type_id_idx` (`result_type_id` ASC) VISIBLE,
  CONSTRAINT `inspections_license_number`
    FOREIGN KEY (`license_number`)
    REFERENCES `chidata`.`licenses` (`license_number`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `inspections_facility_address_id`
    FOREIGN KEY (`facility_address_id`)
    REFERENCES `chidata`.`facility_addresses` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `inspections_inspection_type_id`
    FOREIGN KEY (`inspection_type_id`)
    REFERENCES `chidata`.`inspection_types` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `inspections_result_type_id`
    FOREIGN KEY (`result_type_id`)
    REFERENCES `chidata`.`result_types` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `chidata`.`violation_types`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `chidata`.`violation_types` ;

CREATE TABLE IF NOT EXISTS `chidata`.`violation_types` (
  `id` INT NOT NULL,
  `name` VARCHAR(93) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `chidata`.`violations`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `chidata`.`violations` ;

CREATE TABLE IF NOT EXISTS `chidata`.`violations` (
  `id` INT NOT NULL,
  `inspection_id` INT NULL,
  `violation_type_id` INT NULL,
  `comment` VARCHAR(5500) NULL,
  PRIMARY KEY (`id`),
  INDEX `inspection_id_idx` (`inspection_id` ASC) VISIBLE,
  INDEX `violation_type_id_idx` (`violation_type_id` ASC) VISIBLE,
  CONSTRAINT `violations_inspection_id`
    FOREIGN KEY (`inspection_id`)
    REFERENCES `chidata`.`inspections` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `violations_violation_type_id`
    FOREIGN KEY (`violation_type_id`)
    REFERENCES `chidata`.`violation_types` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
