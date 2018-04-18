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

use Isotope\Isotope;
use Isotope\Model\Product;
use Isotope\Model\ProductCategory;
use Isotope\Model\ProductPrice;
use Isotope\Model\Attribute;
use Isotope\Backend\Product\Price;
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
class ProductImport extends \BackendModule
{
	/**
	 * Template
	 * @var string
	 */
	protected $strTemplate = 'iso_product_import';

	public $vendors = array();

    public $pages = array();

    public $seeds = array();

    public $chmod = array('u1', 'u2', 'u3', 'u4', 'u5', 'u6', 'g4', 'g5', 'g6');

    public $counter = 1;
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

		if (\Input::post('FORM_SUBMIT') == 'iso_product_import')
		{
			#if (!\Input::post('confirm'))
			#{
				$arrUploaded = $objUploader->uploadTo('system/tmp');

				if (empty($arrUploaded))
				{
					\Message::addError($GLOBALS['TL_LANG']['ERR']['all_fields']);
					$this->reload();
				}

				$arrFiles = array();

				foreach ($arrUploaded as $strFile)
				{
					// Skip folders
					if (is_dir(TL_ROOT . '/' . $strFile))
					{
						\Message::addError(sprintf($GLOBALS['TL_LANG']['ERR']['importFolder'], basename($strFile)));
						continue;
					}

					#$objFile = new \File($strFile, true);

					$arrFiles[] = $strFile;
				}
			#}
			#else
			#{
			#	$arrFiles = explode(',', $this->Session->get('uploaded_files'));
			#}

			// Check whether there are any files
			if (empty($arrFiles))
			{
				\Message::addError($GLOBALS['TL_LANG']['ERR']['all_fields']);
				$this->reload();
			}

			// Proceed
			#if (\Input::post('confirm') == 1)
			#{
			switch(\Input::post('import_type'))
			{
				case 'products':
					$this->importProducts($arrFiles);
					break;
				case 'vendors':
					$this->importVendors($arrFiles);
					break;
				case 'promos':
					$this->importPromos($arrFiles);
					break;
			}
			#}
			
		}
		
		$arrImportTypes['products'] 	= 'Product';
		$arrImportTypes['vendors']		= 'Vendors';
		$arrImportTypes['promos']		= 'Promos';
	
		$this->Template->importTypes = $arrImportTypes;
		$this->Template->action = ampersand(\Environment::get('request'));
		$this->Template->messages = \Message::generate();
		$this->Template->href = $this->getReferer(true);
		$this->Template->title = specialchars($GLOBALS['TL_LANG']['MSC']['backBTTitle']);
		$this->Template->button = $GLOBALS['TL_LANG']['MSC']['backBT'];
		$this->Template->headline = $GLOBALS['TL_LANG']['MSC']['importTitle'];
		$this->Template->uploader =  '<h3>File Source</h3>'.$objUploader->generateMarkup();
		$this->Template->submitButton = specialchars($GLOBALS['TL_LANG']['MSC']['continue']);
		$this->Template->overwriteLabel = $GLOBALS['TL_LANG']['MSC']['overwriteData'];
		$this->Template->importTypeLabel = $GLOBALS['TL_LANG']['MSC']['importType'];
		$this->Template->import = $GLOBALS['TL_LANG']['MSC']['import'];
		$this->Template->maxfilesize = $GLOBALS['TL_CONFIG']['maxFileSize'];
 		
	}

	protected function importProducts($arrFiles)
	{	
		$obj = $this;
		
		// Store the field names of the theme tables
		$arrDbFields = array
		(
			'tl_iso_product'       => $this->Database->getFieldNames('tl_iso_product'),
		);
						
		if(\Input::post('overwrite_data')=='1')
		{								
			\Database::getInstance()->executeUncached("TRUNCATE tl_iso_product");	
			\Database::getInstance()->executeUncached("TRUNCATE tl_iso_product_pricetier");	
			\Database::getInstance()->executeUncached("TRUNCATE tl_iso_product_price");	
			\Database::getInstance()->executeUncached("TRUNCATE tl_iso_product_category");			
		}	
		
		$this->loadVendors();
		
		// As you can see, the first names are not capitalized correctly. Let's fix
		// that with a value converter:
		$converterCurrency = new CallbackValueConverter(function ($input) {
			preg_match_all("/([0-9\.]+)/",str_replace(',','',$input),$arrMatches);
			return (float)$arrMatches[0][0];
		});		
		
		$converterFloat = new CallbackValueConverter(function ($input) {
			return (float)$input;	
		});
		
		$converterDiscount = new CallbackValueConverter(function ($input) use ($obj) {
			return $obj->convertDiscount($input);	
		});
		
		$converterInt = new CallbackValueConverter(function ($input) {
			return (integer)$input;	
		});
		
		$converterAlias = new CallbackValueConverter(function ($input) {
			return standardize($input);
		});
		
		$converterVendor = new CallbackValueConverter(function ($input) use ($obj) {
			
			return $obj->vendors[(integer)$input]['id'];
		});
		
		$writerCallback = new CallbackWriter(function ($row) {
			var_dump($row); exit;
		});
		
		$converterRequiredFields = new CallbackItemConverter(function ($item) use ($obj) {
				$item['alias'] = $obj->generateAlias($item['item_number']);
				$item['type'] = 2;
				$item['tstamp'] = time();
				$item['dateAdded'] = time();
				$item['published'] = '1';
				$item['shipping_weight'] = array(0,'kg');
				//$item['product_line'] = $item['product_id'];
				
				//create the hash key
				//$strPromo = standardize(\String::restoreBasicEntities($item['promo_title']));
                //use promo_id field instead

				//$strProductLine = standardize(\String::restoreBasicEntities($item['product_line']));

				$intCatId = $obj->getCategoryByAlias(strtolower('promo-'.$item['promo_id'].'-'.$item['product_id']));

				$item['orderPages'] = serialize(array($intCatId));
				
				return $item;
			});
			
		$converterMapping = new MappingItemConverter();
		
		$converterMapping->addMapping('product_title','name');	
		$converterMapping->addMapping('promo_title','promo');	
		$converterMapping->addMapping('item_number','sku');
		$converterMapping->addMapping('id','product_id');
		$converterMapping->addMapping('dws_net','baseprice');
        $converterMapping->addMapping('vendor_id','vendor');
        $converterMapping->addMapping('promo_id','promo');
        $converterMapping->addMapping('product_id','product_line');
        $converterMapping->addMapping('deal_detail','deal_note');

		$writerProducts = new CallbackWriter(function($row) use ($obj) {
			unset($row['delete']);

			try {				
				$intId = \Database::getInstance()->prepare("INSERT INTO tl_iso_product %s")->set($row)->executeUncached()->insertId;

				$arrCategories = deserialize($row['orderPages'],true);
				
				if(count($arrCategories) && strlen($arrCategories[0]))
				{					
					$obj->saveCategory($arrCategories,$intId);
				}
			}
			catch(\Exception $e)
			{
				echo $e->getMessage(); exit;	
			}

		});
		
		foreach($arrFiles as $file)
		{
		
			$objFile = new \SplFileObject(TL_ROOT . '/' . $file);
	
			$reader = new CsvReader($objFile, "\t");
		
			$reader->setHeaderRowNumber(0);
				
			$workflow = new Workflow($reader);
						
			//need a write but how
			$workflow
				->addValueConverter('msrp', $converterCurrency)
				->addValueConverter('baseprice', $converterCurrency)
				->addValueConverter('discount', $converterDiscount)
				->addValueConverter('min_qty', $converterInt)
				->addValueConverter('ec_compare', $converterDiscount)
				->addValueConverter('product_title', $converterAlias)
                ->addValueConverter('vendor_id',$converterInt)
				#->addValueConverter('vendor_id',$converterVendor)
				->addItemConverter($converterRequiredFields)
				->addItemConverter($converterMapping)
				#->addWriter($writerCallback)
				->addWriter($writerProducts)
				->process();

            //add prices
            try {
                \Database::getInstance()->executeUncached("INSERT INTO tl_iso_product_price (pid,tstamp,tax_class,config_id,member_group) SELECT id, UNIX_TIMESTAMP(NOW()),0,0,0 FROM tl_iso_product");
            }
            catch(\Exception $e)
            {
                echo $e->getMessage(); exit;
            }

            //add prices
            try {
                \Database::getInstance()->executeUncached("INSERT INTO tl_iso_product_pricetier (pid,tstamp,min,price) SELECT id, UNIX_TIMESTAMP(NOW()),1,baseprice FROM tl_iso_product");
            }
            catch(\Exception $e)
            {
                echo $e->getMessage(); exit;
            }

            try {
                \Database::getInstance()->executeUncached("UPDATE `tl_iso_product_collection_item` i, tl_iso_product p SET i.product_id=p.id WHERE i.sku=p.sku");
            }
            catch(\Exception $e)
            {
                echo $e->getMessage(); exit;
            }
		}
		
	}
	
	
	public function generateAlias($strValue,$strAppend=false)
	{		
		$strAlias = standardize(\String::restoreBasicEntities($strValue));
			
		$objAlias = \Database::getInstance()->prepare("SELECT MAX(id), id FROM tl_iso_product WHERE alias=?")
								   ->execute($strAlias);

		// Check whether the product alias exists
		if ($objAlias->numRows)
		{
			$strAlias .= '-' . ($strAppend!==false ? standardize(\String::restoreBasicEntities($strAppend)) : $objAlias->id+1);	
		}

		return $strAlias;
	}
	
	public function getCategoryByAlias($strAlias)
	{
		$objPage = \Database::getInstance()->execute("SELECT id FROM tl_page WHERE alias='$strAlias'");
		
		if($objPage->numRows)
		{
			return $objPage->id;	
		}else{
			\System::log('Alias "' . $strAlias . '" has no matching page', 'ProductImport getCategoryByAlias()', TL_ERROR);	
		}
		
		return false;
	}

    public function getCategoryPages() {
        $objPages = \Database::getInstance()->execute("SELECT id, alias FROM tl_page WHERE vendor>0");

        if($objPages->numRows)
        {
            $arrReturn = array();

            while($objPages->next()) {
                $arrReturn[$objPages->alias] = (integer)$objPages->id;
            }

            return $arrReturn;
        }

        return false;
    }
	
	/**
     * Save page ids to product category table. This allows to retrieve all products associated to a page.
     * @param   mixed
     * @param   DataContainer
     * @return  mixed
     */
    public function saveCategory($arrIds,$intProductId)
    {        
        $table  = ProductCategory::getTable();

        if (is_array($arrIds) && !empty($arrIds)) {
            $time = time();

            \Database::getInstance()->query("DELETE FROM $table WHERE pid=$intProductId AND page_id NOT IN (" . implode(',', $arrIds) . ")")->affectedRows;

            $objPages = \Database::getInstance()->execute("SELECT page_id FROM $table WHERE pid=$intProductId");
            $arrIds   = array_diff($arrIds, $objPages->fetchEach('page_id'));

            if (!empty($arrIds)) {
                foreach ($arrIds as $id) {
                    $sorting = (int) \Database::getInstance()->executeUncached("SELECT MAX(sorting) AS sorting FROM $table WHERE page_id=$id")->sorting + 128;
                    \Database::getInstance()->query("INSERT INTO $table (pid,tstamp,page_id,sorting) VALUES ($intProductId, $time, $id, $sorting)");
                }
            }
        } else {
            \Database::getInstance()->query("DELETE FROM $table WHERE pid=$intProductId");
        }
    }
	
	
	public function savePrice($fltPrice,$intProductId)
	{
		$time = time();

        // Parse the timePeriod widget
       
        $strPrice = (string) $fltPrice;
       	$intTax = 0;
		
		$intPrice = \Database::getInstance()->prepare("
			INSERT INTO " . ProductPrice::getTable() . " (pid,tstamp,tax_class) VALUES (?,?,?)
		")->execute($intProductId, $time, $intTax)->insertId;
		
		\Database::getInstance()->prepare("
			INSERT INTO tl_iso_product_pricetier (pid,tstamp,min,price) VALUES (?,?,1,?)
		")->executeUncached($intPrice, $time, $strPrice);
	}
	
	public function convertDiscount($input)
	{
		if(floatval($input)>1.01)
			return (string)(floatval($input).'%');
		
		return (string)(floatval($input*100).'%');	
	}
	
	public function hashKey($strValue)
	{
		return md5($strValue);	
	}
	
	protected function loadVendors()
	{
		$objResult = \Database::getInstance()->executeUncached("SELECT * FROM tl_vendor");
		
		if($objResult->numRows) {
			$arrVendors = $objResult->fetchAllAssoc();

            foreach($arrVendors as $vendor) {
                $arrResult[$vendor['vendor_id']] = $vendor;
            }

			$this->vendors = $arrResult;
		}
		
	}
	
	public function convertVendorId($id)
	{
			return $this->vendors[$id];
	}
	
	protected function importVendors($arrFiles)
	{	
		#var_dump($arrFiles);
		// Store the field names of the theme tables
		$arrDbFields = array
		(
			'tl_vendor'       => $this->Database->getFieldNames('tl_vendor'),
		);
		
		if(\Input::post('overwrite_data')=='1')
		{								
			\Database::getInstance()->executeUncached("TRUNCATE tl_vendor");			
		}	
		
		foreach($arrFiles as $file)
		{
		
			$objFile = new \SplFileObject(TL_ROOT . '/' . $file);
	
			$reader = new CsvReader($objFile, "\t");
            #$reader->setStrict(false);
			$reader->setHeaderRowNumber(0);

            $workflow = new Workflow($reader);
						
			$converterInt = new CallbackValueConverter(function ($input) {
				return (integer)$input;	
			});
			
			$converterAlias = new CallbackValueConverter(function ($input) {
				return standardize($input);
			});
						
			$converterRequiredFields = new CallbackItemConverter(function ($item) {
				$item['tstamp'] = time();
				return $item;
			});
			
			$converterMapping = new MappingItemConverter();

            // from old column to new column
            $converterMapping->addMapping('vendor_number','vendor_id');
            $converterMapping->addMapping('vendor_name','name');
            $converterMapping->addMapping('vendor_logo','vendor_image');
            $converterMapping->addMapping('vendor_min_type','min_type');
            $converterMapping->addMapping('vendor_min_amt','min_amount');

			$writerVendors = new CallbackWriter(function($row) {
                unset($row['brand']);
                unset($row['delete']);

                try {
					\Database::getInstance()->prepare("INSERT INTO tl_vendor %s")->set($row)->executeUncached();
				}
				catch(\Exception $e)
				{
					echo $e->getMessage();
				}
			});
			
			//need a write but how
			$workflow
				->addItemConverter($converterRequiredFields)
				->addItemConverter($converterMapping)
                ->addValueConverter('id', $converterInt)
                //->addValueConverter('vendor_id',$converterInt)
				->addWriter($writerVendors)
				->process();
		}
		
	}

	protected function importPromos($arrFiles)
	{

        if(\Input::post('overwrite_data')=='1')
        {
            //Get Vendor Pages ONLY
            $objVendorPages = \Database::getInstance()->executeUncached("SELECT id FROM tl_page WHERE vendor>0");

            $arrVendorPageIds = $objVendorPages->fetchEach('id');

            //Get Vendor Page Article IDs ONLY
            $objVendorArticles = \Database::getInstance()->executeUncached("SELECT id FROM tl_article WHERE pid IN(".implode(",",$arrVendorPageIds).")");

            $arrVendorArticleIds = $objVendorArticles->fetchEach('id');

            //remove vendor pages ONLY
            \Database::getInstance()->executeUncached("DELETE FROM tl_page WHERE vendor>0");

            //remove orphaned articles and content elements for vendor page articles ONLY
            \Database::getInstance()->executeUncached("DELETE FROM tl_article WHERE id IN(".implode(",",$arrVendorArticleIds).") FROM tl_page p )");
            \Database::getInstance()->executeUncached("DELETE FROM tl_content WHERE pid IN(".implode(",",$arrVendorArticleIds).") AND ptable='tl_article'");

        }

        $this->loadVendors();

        //set initial sorting seeds
        $this->seeds['page'] = $this->getMaxSort('tl_page',' WHERE vendor=0');
        $this->seeds['article'] = $this->getMaxSort('tl_article');
        $this->seeds['content'] = $this->getMaxSort('tl_content');

        foreach($arrFiles as $file)
		{
			$obj = $this;
			// Read File
			$objFile = new \SplFileObject(TL_ROOT . '/' . $file);
			$reader = new CsvReader($objFile, "\t");
			$reader->setHeaderRowNumber(0);

			// Build Workflow and Processing Functions
			$workflow = new Workflow($reader);

			$converterMapping = new MappingItemConverter();
			
			$converterMapping->addMapping('id','vendor_id');
			$converterMapping->addMapping('name','vendor_name');
			$converterMapping->addMapping('teaser_copy','vendor_teaser');
			$converterMapping->addMapping('subtitle','promo_subtitle');	
			$converterMapping->addMapping('catalog_copy','description');
			$converterMapping->addMapping('rack_dimensions','dimensions');
			$converterMapping->addMapping('catalog_link','item_link');
			$converterMapping->addMapping('deal_note','deal_detail');
			$converterMapping->addMapping('availability_date','availability');
            //$converterMapping->addMapping('sorting','promo_sort');

            //actual table fields
            /*array('id', 'tstamp', 'title', 'alias', 'pid', 'vendor_name', 'fulfillment_type', 'feature_type',
    'promo_sort', 'promo_id', 'promo_title', 'product_sort', 'product_id', 'catalog_copy', 'catalog_link',
    'rack_dimensions', 'vendor_image', 'image_path', 'availability_date', 'teaser_copy', 'subtitle',
    'deal_short_description', 'workshop_deal', 'ec_compare', 'terms', 'deal_note', 'feature_benefit_1',
    'feature_benefit_2', 'feature_benefit_3', 'feature_benefit_4');*/

			$converterPercent = new CallbackValueConverter(function ($input) {
				$output = (float)$input * 100;
				$output = number_format($output, 0);
				return $output;	
			});
				
			$converterFloat = new CallbackValueConverter(function ($input) {
				return (float)$input;	
			});
			
			$converterInt = new CallbackValueConverter(function ($input) {
				return (integer)$input;	
			});
			
			$converterAlias = new CallbackValueConverter(function ($input) {
				return standardize($input);
			});
			
			$converterPromoAlias = new CallbackItemConverter(function ($item) {
				$item['alias'] = standardize("promo-" .$item['promo_id']);
				return $item;
			});

			$converterProductAlias = new CallbackItemConverter(function ($item) {
				$item['product_alias'] = standardize($item['alias'] ."-" .$item['product_id']);
				$item['category_hash_key'] = $item['product_alias'];
				return $item;
			});
			
			$converterPageName = new CallbackItemConverter(function ($item) {
				$item['page_name'] = $item["promo_title"];
				return $item;
			});

			$converterVendorAlias = new CallbackItemConverter(function ($item) {
				$item['vendor_alias'] = standardize($item["vendor_id"]);
				return $item;
			});

			$converterItemNumber = new CallbackItemConverter(function ($item) {
				$query = parse_url($item["item_link"], PHP_URL_QUERY);
				$arrQuery = parse_str($query);
				$itemNumber = $arrQuery["product"];
				$item['item_number'] = $itemNumber;			
				return $item;
			});

			$converterParseTemplates = new CallbackItemConverter(function ($item) use ($obj) {

                //get the vendor data from the pre-loaded vendors array
                $item['vendor_record'] = $this->vendors[$item['vendor_id']];

				$template = new \FrontendTemplate('iso_promo_header');
				$template->promoAlias = $item['alias'];
				$template->promoTitle = $item['promo_title'];
                $template->promoSubtitle = $item['promo_description'];
				$item["promo_header_html"] = $template->parse();

                //set these to false so we don't create empty articles
                $item["vendor_html"]['main'] = false;
                $item["vendor_html"]['left'] = false;

                //extra vendor related info
                if(is_array($item['vendor_record'])) {
                    //Main column content
                    $template = new \FrontendTemplate('iso_promo_vendor_main');
                    $template->promoAlias = $item['alias'];

                    #$template->vendorName = $item['vendor_name'];

                    #$template->vendorTeaser = $item['vendor_teaser'];
                    $template->vendorDetails = $item['vendor_record']['vendor_details'];
                    $template->vendorDropshipMinimum = $item['vendor_record']['order_minimum'];
                    $template->vendorFreightPolicy = $item['vendor_record']['freight_policy'];

                    $template->hasSpecialOffer = ($item['vendor_record']['special_DC_offer']!='' && $item['vendor_record']['special_DC_min']!='' ? true : false);

                    if ($template->hasSpecialOffer) {
                        $template->vendorDcOffer = $item['vendor_record']['special_DC_offer'];
                        $template->vendorDcMin = $item['vendor_record']['special_DC_min'];
                    }

                    $item["vendor_html"]['main'] = $template->parse();

                    //Left column content
                    $template = new \FrontendTemplate('iso_promo_vendor_left');

                    if($item['vendor_record']['vendor_image']!='') {
                        $template->promoAlias = $item['alias'];
                        $template->vendorImage = $item['vendor_record']['vendor_image'];
                        $template->parentPage = 'id-'.$item['vendor_id'].'.html';
                        $item["vendor_html"]['left'] = $template->parse();
                    }else{
                        $item["vendor_html"]["left"] = ucfirst(strtolower($item['vendor_record']['name']));
                    }
                }

                //Main column
				$template = new \FrontendTemplate('iso_promo_category');
				$template->featured = ($item['feature_type']==1 || $item['feature_type']=='Featured' || $item['feature_type']=='FEATURED' ? true : false);
				$template->imageUrl = $item['image_path'];
				$template->productAlias = $item['product_alias'];
				$template->productTitle = $item['product_title'];
				$template->description = iconv("utf-8", "utf-8//ignore", $item['description']);
				$template->dimensions = $item['dimensions'];
				$template->itemLink = $item['item_link'];
				$template->itemNumber = $item['item_number'];
				$template->workshopDeal = $item['workshop_deal'];
				$template->ecCompare = $obj->convertDiscount($item['ec_compare']);
				$template->terms = $item['terms'];
				$template->dealDetail = $item['deal_detail'];
				$template->availability = $item['availability'];
				$template->featuresAndBenefits = $obj->generateFeatures($item);
				$item["promo_category_html"]["main"] = $template->parse();

                //Left column
                if(is_array($item['vendor_record'])) {
                    $template = new \FrontendTemplate('iso_promo_category_left');
                    $template->vendorDetails = $item['vendor_record']['vendor_details'];
                    $template->orderMinimum = $item['vendor_record']['order_minimum'];
                    $template->freightPolicy = $item['vendor_record']['freight_policy'];

                    $template->hasSpecialOffer = ($item['vendor_record']['special_DC_offer']!='' && $item['vendor_record']['special_DC_min']!='' ? true : false);

                    if ($template->hasSpecialOffer) {
                        $template->vendorDcOffer = $item['vendor_record']['special_DC_offer'];
                        $template->vendorDcMin = $item['vendor_record']['special_DC_min'];
                    }

                    $item["promo_category_html"]["left"] = $template->parse();
                }else{
                    $item["promo_category_html"]["left"] = '';
                }

				return $item;
			});

            //Do nothing but build clean arrays we can use to directly insert into the database
            $writerScaffoldDataUpdates = new CallbackWriter(function($row) use ($obj) {

                //If the vendor id doesn't exist in vendors, bounce out.
                if(!array_key_exists($row['vendor_id'],$this->vendors)) {

                    return false;
                }

                //page payload for doing out query work.  First we insert a page, then we take the insert ID and apply
                //it as a PID for the articles, and in turn for the Content elements from the article IDs. More of a
                //recursive approach for efficiency

                //only set the vendor page if it doesn't exist.
                if(!array_key_exists($row['vendor_id'],$obj->pages)) {

                    $obj->seeds['vendor'][$row['vendor_id']] = $obj->seeds['page'] += 128;

                    $obj->pages[$row['vendor_id']]['vendor'] = array(
                        'data'  => array(
                            'pid' => 5,
                            'tstamp' => time(),
                            'alias' => $row['vendor_alias'],
                            'title' => $row['vendor_name'],
                            'pageTitle' => $row['vendor_name'],
                            'vendor' => $row['vendor_id'],
                            'chmod' => $this->chmod,
                            'sorting' => $obj->seeds['vendor'][$row['vendor_id']],
                            'type' => 'regular',
                            'includelayout' => '1',
                            'layout' => 7,   //7 is the layout id for vendor pages
                            'robots' => 'noindex,nofollow',
                            'redirect' => 'permanent',
                            'sitemap' => 'map_default',
                            'published' => '1'
                        ),
                        'articles' =>
                        array(
                            array(
                                'data'  => array(
                                    'pid' => 0,   //we will procure the page id before adding this
                                    'sorting' => $obj->seeds['article'] += 128,
                                    'tstamp' => time(),
                                    'alias' => $row['vendor_alias'],
                                    'title' => $row['vendor_name'],
                                    'inColumn' => 'left',
                                    'author' => 2,
                                    'published' => '1'
                                ),
                                'content_elements' => array(array(
                                    'pid'       => 0,
                                    'ptable'    => 'tl_article',
                                    'tstamp'    => time(),
                                    'type'      => 'text',
                                    'headline'  => '',
                                    'text'      => $row['vendor_html']['left'],
                                    'category_hash_key' => "vendor-logo-" . $row['vendor_alias']
                                ))
                            ),
                            array(
                                'data'  => array(
                                    'pid' => 0,   //we will procure the article id before adding this
                                    'sorting' => $obj->seeds['content'] += 128,
                                    'tstamp' => time(),
                                    'alias' => '',
                                    'title' => '',
                                    'inColumn' => 'main',
                                    'author' => 2,
                                    'published' => '1'
                                ),
                                'content_elements' => array(array(
                                    'pid'       => 0,
                                    'ptable'    => 'tl_article',
                                    'tstamp'    => time(),
                                    'type'      => 'text',
                                    'headline'  => '',
                                    'text' => $row['vendor_html']['main'],
                                    'category_hash_key' => "vendor-terms-" . $row['vendor_alias']
                                ))
                            )
                        )
                    );
                }

                if(!array_key_exists('promos',$obj->pages[$row['vendor_id']])) {
                    $obj->pages[$row['vendor_id']]['promos'] = array();
                }

                //Add the promo page if it doesn't exist
                if(!array_key_exists($row['promo_id'],$obj->pages[$row['vendor_id']]['promos'])) {

                    $obj->pages[$row['vendor_id']]['promos'][$row['promo_id']] = array(
                        'data'  => array(
                            'pid' => 0,
                            'tstamp' => time(),
                            'alias' => $row['alias'],
                            'title' => $row["page_name"],
                            'pageTitle' => $row["page_name"],
                            'vendor' => $row['vendor_id'],
                            'chmod' => $this->chmod,
                            'sorting' => $obj->seeds['vendor'][$row['vendor_id']] + (integer)$row['promo_sort'],
                            'type' => 'regular',
                            'includelayout' => '1',
                            'layout' => 3,   //3 is the layout id for promo pages
                            'robots' => 'noindex,nofollow',
                            'redirect' => 'permanent',
                            'sitemap' => 'map_default',
                            'published' => '1'
                        ),
                        'articles' =>
                        array(
                            array
                            (
                                    'data'  => array(
                                        'pid' => 0,   //we will procure the page id before adding this
                                        'sorting' => $obj->seeds['article'] += 128,
                                        'tstamp' => time(),
                                        'alias' => $row['vendor_alias'],
                                        'title' => $row['vendor_name'],
                                        'inColumn' => 'left',
                                        'author' => 2,
                                        'published' => '1'
                                    ),
                                    'content_elements' => array(
                                        array(
                                            'pid'       => 0,
                                            'ptable'    => 'tl_article',
                                            'tstamp'    => time(),
                                            'type'      => 'text',
                                            'headline'  => '',
                                            'text' => $row['vendor_html']['left'],
                                            'category_hash_key' => "promo-logo-" . $row['vendor_alias']
                                        ),
                                        array(
                                            'pid'       => 0,
                                            'sorting'   => 128,
                                            'ptable'    => 'tl_article',
                                            'tstamp'    => time(),
                                            'type'      => 'html',
                                            'headline'  => '',
                                            'text'      => '',
                                            'html'      => $row['promo_category_html']['left'],
                                            'category_hash_key' => 'promo-deal-info-' . $row['vendor_alias']
                                        )
                                    )
                            ),
                            array(
                                    'data' => array(
                                        'pid' => 0,   //we will procure the article id before adding this
                                        'sorting' => $obj->seeds['content'] += 128,
                                        'tstamp' => time(),
                                        'alias' => '',
                                        'title' => '',
                                        'inColumn' => 'main',
                                        'author' => 2,
                                        'published' => '1'
                                    ),
                                    'content_elements' => array(
                                        array(
                                            'pid'       => 0,
                                            'ptable'    => 'tl_article',
                                            'tstamp'    => time(),
                                            'type'      => 'text',
                                            'headline'  => '',
                                            'text' => $row['promo_header_html'],
                                            'category_hash_key' => "promo-header-" . $row['vendor_alias']
                                        )
                                    )
                            )
                        )
                    );
                }

                //Always add the category page, this is what each row in "promos" represents with its data
                $obj->pages[$row['vendor_id']]['promos'][$row['promo_id']]['categories'][$row['product_id']] = array(
                    'data' => array(
                        'pid' => 0,
                        'tstamp' => time(),
                        'alias' => $row['category_hash_key'],
                        'title' => $row['product_title'],
                        'pageTitle' => $row['product_title'],
                        'vendor' => $row['vendor_id'],
                        'chmod' => $this->chmod,
                        'sorting' => $obj->seeds['vendor'][$row['vendor_id']] + (integer)$row['product_sort'],
                        'type' => 'regular',
                        'includelayout' => '1',
                        'layout' => 3,   //3 is the layout id for promo pages
                        'robots' => 'noindex,nofollow',
                        'redirect' => 'permanent',
                        'sitemap' => 'map_default',
                        'noSearch'  => '1',
                        'published' => '1'
                    ),
                    'articles' =>
                    array(
                        array(
                            'data' => array(
                                'pid' => 0,   //we will procure the page id before adding this
                                'sorting' => $obj->seeds['article'] += 128,
                                'tstamp' => time(),
                                'alias' => $row['vendor_alias'],
                                'title' => $row['vendor_name'],
                                'inColumn' => 'left',
                                'author' => 2,
                                'published' => '1'
                            ),
                            'content_elements' => array(
                                array(
                                    'pid'       => 0,
                                    'ptable'    => 'tl_article',
                                    'tstamp'    => time(),
                                    'type'      => 'text',
                                    'headline'  => '',
                                    'text' => $row['vendor_html']['left'],
                                    'category_hash_key' => 'category-logo-' . $row['vendor_alias']
                                ),
                                array(
                                    'pid'       => 0,
                                    'ptable'    => 'tl_article',
                                    'sorting'   => 128,
                                    'tstamp'    => time(),
                                    'type'      => 'html',
                                    'headline'  => '',
                                    'text'      => '',
                                    'html'      => $row['promo_category_html']['left'],
                                    'category_hash_key' => 'category-deal-info-' . $row['vendor_alias']
                                )
                            )
                        ),
                        array(
                            'data' => array(
                                'pid' => 0,   //we will procure the article id before adding this
                                'sorting' => $obj->seeds['content'] += 128,
                                'tstamp' => time(),
                                'alias' => '',
                                'title' => '',
                                'inColumn' => 'main',
                                'author' => 2,
                                'published' => '1'
                            ),
                            'content_elements' => array(
                                array(
                                    'pid'       => 0,
                                    'ptable'    => 'tl_article',
                                    'tstamp'    => time(),
                                    'type'      => 'text',
                                    'headline'  => '',
                                    'text' => $row["promo_category_html"]["main"],
                                    'category_hash_key' => 'category-header-' . $row['vendor_alias']
                                )
                            )
                        )
                    )
                );

            });

                    $workflow
				->addValueConverter('ec_compare', $converterPercent)
				->addValueConverter('id', $converterInt)
				->addItemConverter($converterPromoAlias)
				->addItemConverter($converterProductAlias)
				->addItemConverter($converterItemNumber)
				->addItemConverter($converterMapping)
				->addItemConverter($converterPageName)
				->addItemConverter($converterVendorAlias)
				->addItemConverter($converterParseTemplates)
				->addWriter($writerScaffoldDataUpdates)
				->process();

            //run the full page & content updates here in bulk instead of 1 at a time in a writer
            if(count($obj->pages)) {

                $arrInserts = array();
                $arrUpdates = array();
                $arrDeletes = array();

                $this->createVendors();

                //our main holding tank for all pages and related content.  We group pages and content together
                //so we can obtain any insert IDs or existing PIDs for proper SQL runs.

                    #$intPage = $this->findPageByVendorId($page['vendor_id'], $page['vendor_alias']);

                    //sort page work by task
                    /*if ($page['delete']) {
                        $arrDeletes[] = $intPage;
                    } else {*/

                //
                /*if(count($arrDeletes)) {
                    $this->processDeletedContent($arrDeletes);
                }

                if(count($arrUpdates)) {
                    $this->processUpdates($arrUpdates);
                }

                if(count($arrInserts)) {
                    $this->processInsertedContent($arrInserts);
                }*/
            }

            //if products exist, re-bind to new category ids
            try {

                $arrUpdate = $arrStatements = array();

                $objResult = \Database::getInstance()->prepare("SELECT id, product_line, promo FROM tl_iso_product")
                    ->execute();

                if($objResult->numRows) {

                    $arrCategories = $this->getCategoryPages();

                    if(\Input::post('overwrite_data')=='1')
                        \Database::getInstance()->executeUncached("TRUNCATE tl_iso_product_category");

                    if (is_array($arrCategories)) {
                        while ($objResult->next()) {

                            $intCatId = $arrCategories[strtolower('promo-' . $objResult->promo . '-' . $objResult->product_line)];

                            if($intCatId>0) {

                                $arrCategories = array($intCatId);
                                $this->saveCategory($arrCategories,(integer)$objResult->id);

                                $arrUpdate[$objResult->id] = serialize(array((string)$intCatId));
                            }

                        }
                    }

                    if (count($arrUpdate)) {
                        foreach ($arrUpdate as $i => $v) {
                            $arrStatements[] = "WHEN id=" . $i . " THEN '" . $v . "'";
                        }
                    }

                    $strStatement = implode("\n", $arrStatements);

                    \Database::getInstance()->executeUncached("UPDATE tl_iso_product SET orderPages = CASE " . $strStatement . " END;");
                }
            }catch (Exception $e) {
                echo $e->getMessage(); exit;
            }

		}

	}

    //Vendors, Promos, Categories workflow, obtaining PID from each parent entity along the way.
    public function createVendors() {
        foreach($this->pages as $k=>$vendor) {

            //Vendor parent record, of which there can only be one.
            $intVendorId = $this->createPage($vendor['vendor']['data']);

            //Vendor articles
            foreach($vendor['vendor']['articles'] as $article) {

                $article['data']['pid'] = $intVendorId;

                $intVendorArticleId = $this->createArticle($article['data']);

                foreach($article['content_elements'] as $content) {
                    if($content[$content['type']]!=='' || $content[$content['type']]=="0") {
                        $content['pid'] = $intVendorArticleId;
                        $this->createContent($content);
                    }
                }
            }

            //Promos, many for each vendor.
            foreach($vendor['promos'] as $promo) {
                //one page per promo, vendor id is parent id.

                $promo['data']['pid'] = $intVendorId;

                $intPromoId = $this->createPage($promo['data']);

                //promo articles, promo id is a page id parent for article as well
                foreach($promo['articles'] as $article) {

                    $article['data']['pid'] = $intPromoId;

                    $intPromoArticleId = $this->createArticle($article['data']);

                    //content for each article in the current promo.
                    foreach($article['content_elements'] as $content) {
                        if($content[$content['type']]!=='' || $content[$content['type']]=="0") {
                            $content['pid'] = $intPromoArticleId;
                            $this->createContent($content);
                        }
                    }
                }

                //Categories, many for each promo.
                foreach($promo['categories'] as $category) {

                    $category['data']['pid'] = $intPromoId;
                    $intCategoryId = $this->createPage($category['data']);

                    //promo articles, promo id is a page id parent for article as well
                    foreach($category['articles'] as $article) {

                        $article['data']['pid'] = $intCategoryId;
                        $intCategoryArticleId = $this->createArticle($article['data']);

                        //content for each article in the current promo.
                        foreach($article['content_elements'] as $content) {
                            if($content[$content['type']]!=='' || $content[$content['type']]=="0") {
                                $content['pid'] = $intCategoryArticleId;
                                $this->createContent($content);
                            }
                        }
                    }
                }
            }

        }
    }

    public function createPage($arrData) {

        $strFields = implode(",",array_keys($arrData));

        for($i=0;$i<count(array_keys($arrData));$i++) {
            $arrPattern[] = "?";
        }

        $strPattern = implode(",",$arrPattern);

        return \Database::getInstance()->prepare("INSERT INTO tl_page ($strFields)VALUES ($strPattern)")
            ->execute($arrData)->insertId;

    }

    public function createArticle($arrData) {

        $strFields = implode(",",array_keys($arrData));

        for($i=0;$i<count(array_keys($arrData));$i++) {
            $arrPattern[] = "?";
        }

        $strPattern = implode(",",$arrPattern);

        return \Database::getInstance()->prepare("INSERT INTO tl_article ($strFields)VALUES ($strPattern)")
            ->execute($arrData)->insertId;
    }

    public function createContent($arrData) {

        $strFields = implode(",",array_keys($arrData));

        for($i=0;$i<count(array_keys($arrData));$i++) {
            $arrPattern[] = "?";
        }

        $strPattern = implode(",",$arrPattern);

        \Database::getInstance()->prepare("INSERT INTO tl_content ($strFields)VALUES ($strPattern)")
            ->execute($arrData);
    }

    public function getMaxSort($strTable,$strWhere='') {
        return \Database::getInstance()->executeUncached("SELECT MAX(sorting) AS sort FROM $strTable".$strWhere)->first()->fetchRow()->sort;

    }

    //Take the complex page & content arrays and break them down for batch SQL
    public function processInserts($arrInserts) {

    }

    //Take the complex page & content arrays and break them down for batch SQL
    public function processUpdates($arrUpdates) {

    }

    public function createContentElementText($intArticleId,$strContent,$strHashKey) {
        // Add Vendor Element
        $insertId = \Database::getInstance()->prepare("INSERT INTO tl_content (pid,ptable,tstamp,type,headline,text,category_hash_key) VALUES (?, 'tl_article', ?, 'text', ?, ?, ?)")
            ->execute($intArticleId, time(), '', $strContent, $strHashKey);

        return $insertId;
    }

    /* Run bulk update queries for pages & related content
        @param array $arrUpdates
        @param string $strTable
        @param string $strUpdateField
    */
    public function batchUpdate($arrUpdates,$strTable,$strUpdateField) {

        foreach ($arrUpdates as $i => $v) {
            $arrStatements[] = "WHEN id=" . $i . " THEN '" . $v . "'";
        }

        $strStatement = implode("\n", $arrStatements);

        \Database::getInstance()->executeUncached("UPDATE $strTable SET $strUpdateField = CASE " . $strStatement . " END;");
    }

    /* Run bulk insert queries for pages & related content
        @param array $arrInserts
        @param string $strTable
    */
    public function batchInsert($arrInserts,$strTable) {

        $arrInsertRows = array();

        //build the batch statement components
        foreach($arrInserts as $row) {
            $arrInsertRows[] = implode(",",$row);
        }

        if(count($arrInsertRows)) {
            //get the field names
            $strKeys = implode(array_keys(current($arrInserts)));

            $strStatements = implode("),(",$arrInsertRows);
            \Database::getInstance()->executeUncached("INSERT INTO $strTable ($strKeys)VALUES($strStatements)");
        }

    }

    /*
    Clean up all pages and related content marked as delete, remove product-category assocations, proper housekeeping.
    */
    public function processDeletedContent($arrDeletes) {
        $strIds = implode(",",$arrDeletes);
        \Database::getInstance()->executeUncached("DELETE FROM tl_page WHERE id IN($strIds)");

        //remove orphaned articles and content
        \Database::getInstance()->executeUncached("DELETE FROM tl_article WHERE pid NOT IN(SELECT id FROM tl_page)");
        \Database::getInstance()->executeUncached("DELETE FROM tl_content WHERE ptable='tl_article' AND pid NOT IN(SELECT id FROM tl_article)");

        //remove product category associations
        \Database::getInstance()->executeUncached("DELETE FROM tl_iso_product_category WHERE page_id NOT IN(SELECT id FROM tl_page) OR pid NOT IN(SELECT id FROM tl_iso_product)");
        \Database::getInstance()->executeUncached("UPDATE tl_iso_product SET orderPages=NULL WHERE orderPages IS NOT NULL AND id NOT IN(SELECT pid FROM tl_iso_product_category)");
    }

    public function addVendorLogo($intArticleId,$row) {
            //echo 'vendor article: ' . $intVendorArticleId . ' :: ';

            $intContentId = $this->findContentByVendor($intArticleId,$row['vendor_html']['left'],$row['vendor_alias']);

            //field can't be null
            $arrHeadline = '';

            //Find the content element
            if($intContentId!==false) {
                //Create new version
                $objVersions = new Versions('tl_content', $intContentId);
                $objVersions->initialize();

                $this->Database->prepare("UPDATE tl_content SET tstamp=". time() .", text=".$row['vendor_html']['left']."  WHERE id=?")
                    ->execute($intContentId);

                $objVersions->create();

                $this->log('A new version of record "tl_content.id='.$intContentId.'" has been created', __METHOD__, TL_GENERAL);

            }else{
                // Add Vendor Element
                \Database::getInstance()->prepare("INSERT INTO tl_content (pid,ptable,tstamp,type,headline,text,category_hash_key) VALUES (?, 'tl_article', ?, 'text', ?, ?, ?)")
                    ->execute($intArticleId, time(), $arrHeadline, $row['vendor_html']['left'], "logo-".$row['vendor_alias']);
            }

    }

    public function findPageByVendorId($intVendorId,$strVendorAlias) {

        $objPage = \Database::getInstance()->prepare("SELECT id FROM tl_page WHERE vendor=? AND pid=5 AND alias LIKE ?")
            ->execute($intVendorId, $strVendorAlias);

        return $objPage->numRows>0 ? $objPage->id : false;
    }

    public function findArticle($intPageId,$strColumn) {
        // Lookup Left Hand Article. If doesn't exist, add it.
        $objArticle = \Database::getInstance()->prepare("SELECT id FROM tl_article WHERE pid=? AND inColumn=?")
            ->execute($intPageId,$strColumn);

        return $objArticle->numRows>0 ? $objArticle->id : false;
    }

    public function findContentByVendor($intArticleId,$strContent,$strAlias) {

        // Check to see if there is a content element for the vendor already. If different, drop it and store new version.
        $objContent = \Database::getInstance()->prepare("SELECT id FROM tl_content WHERE pid=? AND text LIKE ? AND category_hash_key LIKE ? AND type LIKE 'text'")
            ->execute($intArticleId,$strContent,$strAlias);

        return $objContent->numRows>0 ? $objContent->id : false;

    }

	public function generateFeatures($arrItem)
	{
		$arrReturn = array();
		
		$i=1;
		
		foreach($arrItem as $k=>$item)
		{			
			if($k=='feature_benefit_'.$i && (bool)$item!==false)
			{	
				$arrReturn[] = $item;
				$i++;
			}
		}
		
		return $arrReturn;	
	}
	
	/*public function createPage($intPid, $strAlias, $strPageName, $strPageTitle, $strArticleAlias, $strArticleTitle, $intVendorId, $intLayoutId = 0) {
		try {
			$this->chmod = array('u1', 'u2', 'u3', 'u4', 'u5', 'u6', 'g4', 'g5', 'g6');
			
			$intSorting = \Database::getInstance()->prepare("SELECT sorting FROM tl_page WHERE id=?")
							->execute($intPid)->sorting;
			
			$intPageSorting = (int)$intSorting * 2;
			$intArticleSorting = $intPageSorting * 2;

            $includeLayout = ($intLayoutId>0 ? '1' : ' ');

			$intPageId = \Database::getInstance()->prepare("INSERT INTO tl_page (
								pid, tstamp, alias, title, pageTitle, vendor, sorting, chmod,
								type, includelayout, layout, robots, redirect, sitemap, published
							) VALUES (
								?, ?, ?, ?, ?, ?, ?, ?,
								'regular', $includeLayout, $intLayoutId, 'noindex,nofollow', 'permanent', 'map_default', 1)")
							->execute($intPid, time(), $strAlias, $strPageName, $strPageTitle, $intVendorId, $intPageSorting, $this->chmod)->insertId;
							
			$intArticleId = \Database::getInstance()->prepare("INSERT INTO tl_article (
								pid, sorting, tstamp, alias, title, 
								author, inColumn, published
							) VALUES ( 
								?, ?, ?, ?, ?, 
								2, 'main', 1)")
							->execute($intPageId, $intArticleSorting, time(), $strArticleAlias, $strArticleTitle)->insertId;
		
			return array($intPageId, $intArticleId);
		}
		catch(\Exception $e)
		{
			die('Caught exception: '.$e->getMessage()."\n");
		}
	}*/

	/*public function createArticle($intPid, $column, $strArticleAlias, $strArticleTitle, $intVendorId) {
		try {
			$this->chmod = array('u1', 'u2', 'u3', 'u4', 'u5', 'u6', 'g4', 'g5', 'g6');
			
			$intSorting = \Database::getInstance()->prepare("SELECT sorting FROM tl_page WHERE id=?")
							->execute($intPid)->sorting;
			
			$intArticleSorting = (int)$intSorting * 2;
							
			$intArticleId = \Database::getInstance()->prepare("INSERT INTO tl_article (
								pid, sorting, tstamp, alias, title, inColumn,
								author, published
							) VALUES ( 
								?, ?, ?, ?, ?, ?, 
								2, 1)")
							->execute($intPid, $intArticleSorting, time(), $strArticleAlias, $strArticleTitle, $column)->insertId;
		
			return $intArticleId;
		}
		catch(\Exception $e)
		{
			die('Caught exception: '.$e->getMessage()."\n");
		}
	}*/
		
    /**
     * {@inheritdoc}
     */
    protected function checkUserAccess($module)
    {
        return \BackendUser::getInstance()->isAdmin || \BackendUser::getInstance()->hasAccess($module, 'iso_modules');
    }

}
