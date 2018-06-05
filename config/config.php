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


/**
 * Backend modules
 */
array_insert($GLOBALS['BE_MOD']['isotope'], 3, array
(
    'iso_import' => array
    (	
        'callback'          => 'Isotope\BackendModule\ProductImport',
        'tables'            => array(),
        'icon'              => 'system/modules/isotope/assets/images/application-monitor.png'
    )/*,
    'iso_member_import' => array
    (
        'callback'          => 'Isotope\BackendModule\MemberImport',
        'tables'            => array(),
        'icon'              => 'system/modules/isotope/assets/images/application-monitor.png'
    ),*/
));

/**
 * Models
 */
#$GLOBALS['TL_MODELS'][\Isotope\Model\Rule::getTable()] = 'Isotope\Model\Rule';