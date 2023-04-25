-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema energia
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema energia
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `energia` DEFAULT CHARACTER SET utf8 ;
USE `energia` ;

-- -----------------------------------------------------
-- Table `energia`.`fecha`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `energia`.`fecha` (
  `idfecha` INT NOT NULL AUTO_INCREMENT,
  `fecha` DATE NOT NULL,
  PRIMARY KEY (`idfecha`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `energia`.`nacional_renovable_no_renovable`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `energia`.`nacional_renovable_no_renovable` (
  `id_nacional_renovable_no_renovable` INT NOT NULL AUTO_INCREMENT,
  `porcentaje` INT NOT NULL,
  `tipo_energia` VARCHAR(45) NOT NULL,
  `valor` DECIMAL NOT NULL,
  `fecha_idfecha` INT NOT NULL,
  PRIMARY KEY (`id_nacional_renovable_no_renovable`),
  INDEX `fk_nacional_renovable_no_renovable_fecha_idx` (`fecha_idfecha` ASC) VISIBLE,
  CONSTRAINT `fk_nacional_renovable_no_renovable_fecha`
    FOREIGN KEY (`fecha_idfecha`)
    REFERENCES `energia`.`fecha` (`idfecha`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `energia`.`comunidades`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `energia`.`comunidades` (
  `idcomunidades` INT NOT NULL AUTO_INCREMENT,
  `comunidad` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`idcomunidades`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `energia`.`comunidades_renovable_no_renovable`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `energia`.`comunidades_renovable_no_renovable` (
  `idcomunidad_renovable_no_renovable` INT NOT NULL AUTO_INCREMENT,
  `porcentaje` INT NOT NULL,
  `tipo_energia` VARCHAR(45) NOT NULL,
  `valor` DECIMAL NOT NULL,
  `fecha_idfecha` INT NOT NULL,
  `comunidades_idcomunidades` INT NOT NULL,
  PRIMARY KEY (`idcomunidad_renovable_no_renovable`),
  INDEX `fk_comunidades_renovable_no_renovable_fecha1_idx` (`fecha_idfecha` ASC) VISIBLE,
  INDEX `fk_comunidades_renovable_no_renovable_comunidades1_idx` (`comunidades_idcomunidades` ASC) VISIBLE,
  CONSTRAINT `fk_comunidades_renovable_no_renovable_fecha1`
    FOREIGN KEY (`fecha_idfecha`)
    REFERENCES `energia`.`fecha` (`idfecha`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_comunidades_renovable_no_renovable_comunidades1`
    FOREIGN KEY (`comunidades_idcomunidades`)
    REFERENCES `energia`.`comunidades` (`idcomunidades`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
