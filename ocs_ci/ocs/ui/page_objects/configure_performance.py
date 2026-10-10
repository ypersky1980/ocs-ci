"""Page object for Configure Performance page (RHSTOR-8629)."""

import logging
from typing import List, Optional

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from ocs_ci.ocs.ui.base_ui import BaseUI
from ocs_ci.ocs.ocp import OCP


logger = logging.getLogger(__name__)


class ConfigurePerformancePage(BaseUI):
    """Configure Performance page with Core Storage and MCG sections."""

    # Locators
    CORE_STORAGE_SECTION = (
        By.XPATH,
        "//*[contains(text(), 'Core Storage')]"
        "/ancestor::*[contains(@class, 'section') or contains(@class, 'panel')]",
    )
    MCG_SECTION = (
        By.XPATH,
        "//*[contains(text(), 'Multicloud') or contains(text(), 'MCG')]"
        "/ancestor::*[contains(@class, 'section') or contains(@class, 'panel')]",
    )

    MCG_PROFILE_SELECTOR = (
        By.XPATH,
        "//*[contains(@class, 'c-select') and contains(@class, 'odf-configure-performance__selector')]",
    )
    CORE_STORAGE_SELECTOR = (
        By.XPATH,
        "//select[@aria-label='Resource Profile'] | //*[contains(@class, 'resource-profile')]",
    )
    NODE_ITEM = (
        By.XPATH,
        "//*[contains(@class, 'node') or contains(@class, 'resource-item')]",
    )
    NODE_LIST = (By.XPATH, "//div[contains(@class, 'node-list')]")
    SAVE_BUTTON = (By.XPATH, "//button[contains(text(), 'Save')]")
    CANCEL_BUTTON = (
        By.XPATH,
        "//button[contains(text(), 'Cancel') or contains(text(), 'Discard')]",
    )

    def __init__(self):
        super().__init__()
        logger.info("Initializing ConfigurePerformancePage")

    def is_core_storage_section_present(self) -> bool:
        """Check if Core Storage section is present."""
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(self.CORE_STORAGE_SECTION)
            )
            return True
        except Exception as e:
            logger.warning(f"Core Storage section not found: {e}")
            return False

    def is_mcg_section_present(self) -> bool:
        """Check if MCG section is present."""
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(self.MCG_SECTION)
            )
            return True
        except Exception as e:
            logger.warning(f"MCG section not found: {e}")
            return False

    def verify_sections_separated(self) -> bool:
        """Verify sections are visually separated."""
        try:
            core = self.driver.find_element(*self.CORE_STORAGE_SECTION)
            mcg = self.driver.find_element(*self.MCG_SECTION)
            return (
                core.location["y"] != mcg.location["y"]
                or core.location["x"] != mcg.location["x"]
            )
        except Exception as e:
            logger.warning(f"Could not verify section separation: {e}")
            return False

    def is_core_storage_inline(self) -> bool:
        """Verify Core Storage displays inline (not modal)."""
        try:
            core = self.driver.find_element(*self.CORE_STORAGE_SECTION)
            classes = (core.get_attribute("class") or "").lower()
            return "modal" not in classes and "dialog" not in classes
        except Exception as e:
            logger.warning(f"Could not verify if inline: {e}")
            return True

    def get_core_storage_nodes(self) -> List[dict]:
        """Get nodes from Core Storage section."""
        try:
            core = self.driver.find_element(*self.CORE_STORAGE_SECTION)
            nodes = core.find_elements(By.XPATH, ".//div[contains(@class, 'node')]")
            return [
                {
                    "name": n.find_element(By.XPATH, ".//*[@class='node-name']").text,
                    "labels": n.get_attribute("data-labels") or "",
                    "element": n,
                }
                for n in nodes
            ]
        except Exception as e:
            logger.warning(f"Error getting Core Storage nodes: {e}")
            return []

    def get_mcg_nodes(self) -> List[dict]:
        """Get nodes from MCG section."""
        try:
            mcg = self.driver.find_element(*self.MCG_SECTION)
            nodes = mcg.find_elements(By.XPATH, ".//div[contains(@class, 'node')]")
            return [
                {
                    "name": n.find_element(By.XPATH, ".//*[@class='node-name']").text,
                    "labels": n.get_attribute("data-labels") or "",
                    "element": n,
                }
                for n in nodes
            ]
        except Exception as e:
            logger.warning(f"Error getting MCG nodes: {e}")
            return []

    def verify_all_nodes_have_ocs_label(self, nodes: List[dict]) -> bool:
        """Verify all nodes have OCS label."""
        try:
            for node in nodes:
                labels = (node.get("labels") or "").lower()
                if "ocs" not in labels and "openshift-storage" not in labels:
                    logger.warning(f"Node {node.get('name')} missing OCS label")
                    return False
            return True
        except Exception as e:
            logger.warning(f"Error verifying OCS labels: {e}")
            return False

    def has_node_selection_option(self) -> bool:
        """Check if Core Storage has node selection option."""
        try:
            core = self.driver.find_element(*self.CORE_STORAGE_SECTION)
            elements = core.find_elements(
                By.XPATH,
                ".//input[@type='checkbox' or @type='radio'] | .//button[contains(text(), 'Select')]",
            )
            return len(elements) > 0
        except Exception as e:
            logger.warning(f"Error checking node selection: {e}")
            return False

    def select_mcg_profile(self, profile_name: str) -> bool:
        """Select MCG profile."""
        try:
            selector = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(self.MCG_PROFILE_SELECTOR)
            )
            selector.click()
            option = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        f"//div[contains(@class, 'select-option')] | //li[contains(text(), '{profile_name}')]",
                    )
                )
            )
            option.click()
            return True
        except Exception as e:
            logger.error(f"Failed to select MCG profile: {e}")
            return False

    def select_core_storage_profile(self, profile_name: str) -> bool:
        """Select Core Storage profile."""
        try:
            selector = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(self.CORE_STORAGE_SELECTOR)
            )
            selector.click()
            option = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(
                    (
                        By.XPATH,
                        f"//option[text()='{profile_name}'] | //li[contains(text(), '{profile_name}')]",
                    )
                )
            )
            option.click()
            return True
        except Exception as e:
            logger.error(f"Failed to select Core Storage profile: {e}")
            return False

    def save_configuration(self) -> bool:
        """Save configuration."""
        try:
            save_btn = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(self.SAVE_BUTTON)
            )
            save_btn.click()
            from time import sleep

            sleep(2)
            return True
        except Exception as e:
            logger.error(f"Failed to save: {e}")
            return False

    def cancel_changes(self) -> bool:
        """Cancel changes."""
        try:
            cancel_btn = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(self.CANCEL_BUTTON)
            )
            cancel_btn.click()
            return True
        except Exception as e:
            logger.error(f"Failed to cancel: {e}")
            return False

    def get_mcg_profile_from_cr(self) -> Optional[str]:
        """Get MCG profile from StorageCluster CR."""
        try:
            sc = OCP(kind="StorageCluster", namespace="openshift-storage")
            data = sc.get_resource("ocs-storagecluster")
            return (
                data.get("spec", {})
                .get("multiCloudGateway", {})
                .get("performanceProfile")
            )
        except Exception as e:
            logger.error(f"Error reading MCG profile from CR: {e}")
            return None

    def get_core_storage_profile_from_cr(self) -> Optional[str]:
        """Get Core Storage profile from StorageCluster CR."""
        try:
            sc = OCP(kind="StorageCluster", namespace="openshift-storage")
            data = sc.get_resource("ocs-storagecluster")
            return data.get("spec", {}).get("resourceProfile")
        except Exception as e:
            logger.error(f"Error reading Core Storage profile from CR: {e}")
            return None

    def verify_page_integrity(self) -> bool:
        """Verify page has both sections."""
        try:
            return (
                self.is_core_storage_section_present() and self.is_mcg_section_present()
            )
        except Exception as e:
            logger.error(f"Page integrity check failed: {e}")
            return False

    def get_mcg_profile_from_ui(self) -> Optional[str]:
        """Get selected MCG profile from UI dropdown."""
        try:
            selector = self.driver.find_element(*self.MCG_PROFILE_SELECTOR)
            return selector.text.strip().lower()
        except Exception as e:
            logger.error(f"Error reading MCG profile from UI: {e}")
            return None

    def get_core_storage_profile_from_ui(self) -> Optional[str]:
        """Get selected Core Storage profile from UI dropdown."""
        try:
            selector = self.driver.find_element(*self.CORE_STORAGE_SELECTOR)
            return selector.text.strip().lower()
        except Exception as e:
            logger.error(f"Error reading Core Storage profile from UI: {e}")
            return None
