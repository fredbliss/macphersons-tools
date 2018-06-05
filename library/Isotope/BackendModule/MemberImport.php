<?php

/**
 * Isotope eCommerce for Contao Open Source CMS
 *
 * Copyright (C) 2009-2014 terminal42 gmbh & Isotope eCommerce Workgroup
 *
 * @package    Isotope
 * @link       http://isotopeecommerce.org
 * @license    http://opensource.org/licenses/lgpl-3.0.html
 */

namespace Isotope\BackendModule;

require_once TL_ROOT.'/composer/vendor/autoload.php';

use Ddeboer\DataImport\Workflow;
use Ddeboer\DataImport\Reader;
use Ddeboer\DataImport\Writer;
use Ddeboer\DataImport\Filter;
use Ddeboer\DataImport\Reader\ArrayReader;
use Ddeboer\DataImport\ValueConverter\CallbackValueConverter;
use Ddeboer\DataImport\ItemConverter\CallbackItemConverter;
use Ddeboer\DataImport\ItemConverter\MappingItemConverter;
use Ddeboer\DataImport\Reader\CsvReader;
use Ddeboer\DataImport\Writer\CallbackWriter;
use Ddeboer\DataImport\Writer\DoctrineWriter;	

/**
 * Class ModuleIsotopeSetup
 *
 * Back end module Isotope "setup".
 * @copyright  Isotope eCommerce Workgroup 2009-2012
 * @author     Andreas Schempp <andreas.schempp@terminal42.ch>
 * @author     Fred Bliss <fred.bliss@intelligentspark.com>
 */
class MemberImport extends \BackendModule
{
		/**
	 * Template
	 * @var string
	 */
	protected $strTemplate = 'iso_member_import';

	public $vendors = array();
	
	public $members = array();
	
	/**
     * Generate the module
     * @return string
     */
    public function generate()
    {		
        return parent::generate();
	}
	
  	public function compile()
	{
		$this->loadLanguageFile('default');
		$this->import('BackendUser', 'User');
		$class = $this->User->uploader;
		
		// See #4086
		if (!class_exists($class))
		{
			$class = 'FileUpload';
		}

		$objUploader = new $class();
		
		$objUploader2 = new $class();
		
		if (\Input::post('FORM_SUBMIT') == 'iso_member_import')
		{
			$arrFiles = $objUploader->uploadTo('system/tmp');
							
			if (empty($arrFiles))
			{
				\Message::addError($GLOBALS['TL_LANG']['ERR']['all_fields']);
				$this->reload();
			}

			foreach ($arrFiles as $strFile)
			{
				// Skip folders
				if (is_dir(TL_ROOT . '/' . $strFile))
				{
					\Message::addError(sprintf($GLOBALS['TL_LANG']['ERR']['importFolder'], basename($strFile)));
					continue;
				}
				
				
				$arrUploads[] = $strFile;
			}

			// Check whether there are any files
			if (empty($arrFiles))
			{
				\Message::addError($GLOBALS['TL_LANG']['ERR']['all_fields']);
				$this->reload();
			}

			$this->importMembers($arrUploads);
		}
		
		$this->Template->action = ampersand(\Environment::get('request'));
		$this->Template->messages = \Message::generate();
		$this->Template->href = $this->getReferer(true);
		$this->Template->title = specialchars($GLOBALS['TL_LANG']['MSC']['backBTTitle']);
		$this->Template->button = $GLOBALS['TL_LANG']['MSC']['backBT'];
		$this->Template->headline = $GLOBALS['TL_LANG']['MSC']['importTitle'];
		$this->Template->uploaderAccounts =  '<h3>File Source</h3>'.$objUploader->generateMarkup();
		$this->Template->uploaderMembers =  '<h3>File Source</h3>'.$objUploader2->generateMarkup();
		$this->Template->submitButton = specialchars($GLOBALS['TL_LANG']['MSC']['continue']);
		$this->Template->overwriteLabel = $GLOBALS['TL_LANG']['MSC']['overwriteData'];
		$this->Template->accountImportFileLabel = $GLOBALS['TL_LANG']['MSC']['accountFileImport'];
		$this->Template->memberImportFileLabel = $GLOBALS['TL_LANG']['MSC']['memberFileImport'];		
		$this->Template->import = $GLOBALS['TL_LANG']['MSC']['import'];
		$this->Template->maxfilesize = $GLOBALS['TL_CONFIG']['maxFileSize'];
 		
	}
	
	public function importMembers($arrFiles)
	{
		$obj = $this;
				
		// Store the field names of the theme tables
		$arrDbFields = array
		(
			'tl_member'       	=> $this->Database->getFieldNames('tl_member'),
			'tl_iso_address'	=> $this->Database->getFieldNames('tl_iso_address')
		);
		
		if(\Input::post('overwrite_data')=='1')
		{								
			\Database::getInstance()->executeUncached("TRUNCATE tl_member");	
			\Database::getInstance()->executeUncached("TRUNCATE tl_iso_address");
		}
		
		//ISO 3166-2 standard
		$converterSubdivision = new CallbackValueConverter(function ($input) {
			return 'US-'.strtoupper($input);	
		});
		
		$converterContaoGroups = new CallbackValueConverter(function ($input) {
			return serialize(array($input));
		});
		
		$converterRequiredFieldsMembers = new CallbackItemConverter(function ($item) {				
			
			$item['createdOn'] = $item['dateAdded'] = $item['tstamp'] = time();
			$item['login'] = '1';
			$item['country'] = 'us';			
			
			return $item;
		});
		
		$converterRequiredFieldsAddresses = new CallbackItemConverter(function ($item) use (&$obj) {				
			
			$item['tstamp'] = time();
			$item['country'] = 'us';	
			$item['ptable'] = 'tl_member';
			$item['isDefaultShipping'] = ($item['master_account']==$item['shipto_account'] ? '1' : '');
			$item['isDefaultBilling'] = ($item['master_account']==$item['shipto_account'] ? '1' : '');
						
			return $item;
		});
		
		//members
		$converterMappingMembers = new MappingItemConverter();
		
		$converterMappingMembers->addMapping('member_first_name','firstname');
		$converterMappingMembers->addMapping('member_last_name','lastname');
		$converterMappingMembers->addMapping('member_email','email');
		$converterMappingMembers->addMapping('member_phone','phone');
		$converterMappingMembers->addMapping('member_username','username');
		$converterMappingMembers->addMapping('member_group','groups');
		$converterMappingMembers->addMapping('member_company','company');
		$converterMappingMembers->addMapping('member_title','title');
		$converterMappingMembers->addMapping('master_account','macpherson_account');
		
		//addresses
		$converterMappingAddresses = new MappingItemConverter();
		
		$converterMappingAddresses->addMapping('company_name','company');
		$converterMappingAddresses->addMapping('master_account','macpherson_account');
		$converterMappingAddresses->addMapping('shipto_account','macpherson_shipto_account');
		$converterMappingAddresses->addMapping('address1','street_1');
		$converterMappingAddresses->addMapping('address2','street_2');
		$converterMappingAddresses->addMapping('address3','street_3');
		$converterMappingAddresses->addMapping('company_city','city');
		$converterMappingAddresses->addMapping('company_state','subdivision');
		$converterMappingAddresses->addMapping('company_zip','postal');
		#$converterMappingAddresses->addMapping('','');

			
		$writerMembers = new CallbackWriter(function($row) use (&$obj) {
							
			try {				
				$intId = \Database::getInstance()->prepare("INSERT INTO tl_member %s")->set($row)->executeUncached()->insertId;				
			}
			catch(\Exception $e)
			{
				echo $e->getMessage(); exit;	
			}
		});
		
		$writerAddresses = new CallbackWriter(function($row) {
				
			try {								
				$intId = \Database::getInstance()->prepare("INSERT INTO tl_iso_address %s")->set($row)->executeUncached()->insertId;
			}
			catch(\Exception $e)
			{
				echo $e->getMessage(); exit;	
			}
		});
				
		foreach($arrFiles as $i=>$file)
		{			
			$objFile = new \SplFileObject(TL_ROOT . '/' . $file);
	
			$reader = new CsvReader($objFile, "\t");
		
			$reader->setHeaderRowNumber(0);
				
			$workflow = new Workflow($reader);
			
			switch($i)
			{
				case 1:
					$workflow
					->addValueConverter('group',$converterContaoGroups)
					->addItemConverter($converterRequiredFieldsMembers)
					->addItemConverter($converterMappingMembers)
					->addWriter($writerMembers)
					->process();
					break;
				case 0:
					$workflow
					->addValueConverter('subdivision',$converterSubdivision)
					->addItemConverter($converterRequiredFieldsAddresses)
					->addItemConverter($converterMappingAddresses)
					->addWriter($writerAddresses)
					->process();					
					break;
			}
		}
		
		$objMember = \Database::getInstance()->executeUncached("SELECT DISTINCT macpherson_account,id,firstname,lastname,email FROM tl_member WHERE macpherson_account IS NOT NULL");
				
		if($objMember->numRows)	//try to get names for the ship to account
		{
			$arrRows = array();
			
			while($objMember->next())
			{	
			/*							
				$row['pid']	= $objMember->id;
				$row['firstname'] = $objMember->firstname;
				$row['lastname'] = $objMember->lastname;
				$row['email'] = $objMember->email;
				$row['macpherson_account'] = $objMember->macpherson_account;
				*/
				$arrRows[] = $objMember->row();
			}
			
			foreach($arrRows as $row)
			{				
				$arrUpdateField['pid'][] = "WHEN macpherson_account='".$row['macpherson_account']."' THEN ".$row['id'];
				$arrUpdateField['firstname'][] = "WHEN macpherson_account='".$row['macpherson_account']."' THEN '".$row['firstname']."'";
				$arrUpdateField['lastname'][] = "WHEN macpherson_account='".$row['macpherson_account']."' THEN '".$row['lastname']."'";
				$arrUpdateField['email'][] = "WHEN macpherson_account='".$row['macpherson_account']."' THEN '".$row['email']."'";
			}
			
			foreach($arrUpdateField as $field=>$statements)
			{
				$arrStatements[] = $field." = (CASE ".implode("\n",$statements)." END)";					
			}
			
			$strStatements = implode(",",$arrStatements);
			
			\Database::getInstance()->executeUncached("UPDATE tl_iso_address SET $strStatements");
			/*
			SET `variable1` = (CASE
				WHEN `id` = 1 THEN 12
				WHEN `id` = 2 THEN 42
				WHEN `id` = 3 THEN 32
				END),
			`variable2` = (CASE
				WHEN `id` = 1 THEN 'blue'
				WHEN `id` = 2 THEN 'red'
				WHEN `id` = 3 THEN 'yellow'
				END);");
					}*/
		
		}
		
		//create a set of rows to insert that copy master account ship to addresses
		$objAddresses = \Database::getInstance()->executeUncached("SELECT * FROM tl_iso_address WHERE macpherson_account IS NOT NULL");
		$objMembers = \Database::getInstance()->executeUncached("SELECT * FROM tl_member WHERE macpherson_account IS NOT NULL");
		
		while($objMembers->next())
		{		
			$strKey = strval($objMembers->macpherson_account);
					
			$arrMembers[$strKey][] = $objMembers->row();			
		}
		
		$arrAddressesRaw = $objAddresses->fetchAllAssoc();
		$arrAddresses = array();					
		$arrFields = array();
		
		foreach($arrMembers as $k=>$members)
		{
			foreach($members as $member)
			{				
				foreach($arrAddressesRaw as $i=>$address)
				{
					if($address['macpherson_account']!=$k)
						continue; 
						
					$arrMemberAddresses[$member['id']][$i] = $address;
					unset($arrMemberAddresses[$member['id']][$i]['id']);
					$arrMemberAddresses[$member['id']][$i]['pid'] = $member['id'];
					$arrMemberAddresses[$member['id']][$i]['firstname'] = $member['firstname'];
					$arrMemberAddresses[$member['id']][$i]['lastname'] = $member['lastname'];
					$arrMemberAddresses[$member['id']][$i]['email'] = $member['email'];	
										
					if(count($arrFields)==0)	//first time only
						$arrFields = array_keys($arrMemberAddresses[$member['id']][$i]);	
				}
		
				reset($arrAddressesRaw);
			}
		}
				
		$strFields = implode(",",$arrFields); 
		
		$arrRows = array();
		
		error_reporting(0);
	
		foreach($arrMemberAddresses as $k=>$memberAddressCollection)
		{
			#var_dump($memberAddressCollection); exit; 			
			foreach($memberAddressCollection as $address)
			{				
				array_walk($address,'mysql_real_escape_string');
				
				$arrRows[] = implode('","',$address);
			}
			
		}
		
		$strValues = implode('"),("',$arrRows);
				
		try{
			#echo "INSERT IGNORE INTO tl_iso_address ($strFields) VALUES ('$strValues')"; exit;
			\Database::getInstance()->executeUncached("INSERT IGNORE INTO tl_iso_address ($strFields) VALUES (\"$strValues\")");

			//remove duplicates
			\Database::getInstance()->executeUncached("DELETE u1 FROM tl_iso_address u1,
tl_iso_address u2 WHERE u1.id < u2.id AND u1.macpherson_shipto_account = u2.macpherson_shipto_account AND u1.pid = u2.pid");

		}
		catch(\Exception $e)
		{
			echo $e->getMessage(); exit;	
		}
		
	}
	
}