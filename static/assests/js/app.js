'use strict';

/* ===== Enable Bootstrap Popover (on element  ====== */
const popoverTriggerList = document.querySelectorAll('[data-bs-toggle="popover"]')
const popoverList = [...popoverTriggerList].map(popoverTriggerEl => new bootstrap.Popover(popoverTriggerEl))

/* ==== Enable Bootstrap Alert ====== */
//var alertList = document.querySelectorAll('.alert')
//alertList.forEach(function (alert) {
//  new bootstrap.Alert(alert)
//});

const alertList = document.querySelectorAll('.alert')
const alerts = [...alertList].map(element => new bootstrap.Alert(element))


/* ===== Responsive Sidepanel ====== */
const sidePanelToggler = document.getElementById('sidepanel-toggler'); 
const sidePanel = document.getElementById('app-sidepanel');  
const sidePanelDrop = document.getElementById('sidepanel-drop'); 
const sidePanelClose = document.getElementById('sidepanel-close'); 

window.addEventListener('load', function(){
	responsiveSidePanel(); 
});

window.addEventListener('resize', function(){
	responsiveSidePanel(); 
});


function responsiveSidePanel() {
    let w = window.innerWidth;
	if(w >= 1200) {
		// Desktop: restore state from localStorage or default visible
		const collapsed = localStorage.getItem('sidepanel-collapsed') === 'true';
		if (collapsed) {
			sidePanel.classList.remove('sidepanel-visible');
			sidePanel.classList.add('sidepanel-hidden');
			document.body.classList.add('sidepanel-collapsed');
			updateSidepanelChevron(false);
		} else {
			sidePanel.classList.remove('sidepanel-hidden');
			sidePanel.classList.add('sidepanel-visible');
			document.body.classList.remove('sidepanel-collapsed');
			updateSidepanelChevron(true);
		}
	} else {
	    sidePanel.classList.remove('sidepanel-visible');
		sidePanel.classList.add('sidepanel-hidden');
		document.body.classList.remove('sidepanel-collapsed');
		updateSidepanelChevron(true);
	}
}

function updateSidepanelChevron(panelVisible) {
	const iconClose = document.getElementById('sidepanel-toggler-icon-close');
	const iconOpen = document.getElementById('sidepanel-toggler-icon-open');
	if (iconClose && iconOpen) {
		if (panelVisible) {
			iconClose.classList.remove('d-none');
			iconClose.classList.add('d-xl-inline-block');
			iconOpen.classList.add('d-none');
		} else {
			iconClose.classList.add('d-none');
			iconOpen.classList.remove('d-none');
			iconOpen.classList.add('d-xl-inline-block');
		}
	}
}

sidePanelToggler.addEventListener('click', (e) => {
	e.preventDefault();
	if (sidePanel.classList.contains('sidepanel-visible')) {
		sidePanel.classList.remove('sidepanel-visible');
		sidePanel.classList.add('sidepanel-hidden');
		document.body.classList.add('sidepanel-collapsed');
		if (window.innerWidth >= 1200) localStorage.setItem('sidepanel-collapsed', 'true');
		updateSidepanelChevron(false);
	} else {
		sidePanel.classList.remove('sidepanel-hidden');
		sidePanel.classList.add('sidepanel-visible');
		document.body.classList.remove('sidepanel-collapsed');
		if (window.innerWidth >= 1200) localStorage.setItem('sidepanel-collapsed', 'false');
		updateSidepanelChevron(true);
	}
});



sidePanelClose.addEventListener('click', (e) => {
	e.preventDefault();
	sidePanelToggler.click();
});

sidePanelDrop.addEventListener('click', (e) => {
	sidePanelToggler.click();
});



/* ====== Mobile search ======= */
const searchMobileTrigger = document.querySelector('.search-mobile-trigger');
const searchBox = document.querySelector('.app-search-box');

searchMobileTrigger.addEventListener('click', () => {

	searchBox.classList.toggle('is-visible');
	
	let searchMobileTriggerIcon = document.querySelector('.search-mobile-trigger-icon');
	
	if(searchMobileTriggerIcon.classList.contains('fa-magnifying-glass')) {
		searchMobileTriggerIcon.classList.remove('fa-magnifying-glass');
		searchMobileTriggerIcon.classList.add('fa-xmark');
	} else {
		searchMobileTriggerIcon.classList.remove('fa-xmark');
		searchMobileTriggerIcon.classList.add('fa-magnifying-glass');
	}
	
		
	
});


